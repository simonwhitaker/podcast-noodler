import asyncio
import json
import os
import re

import aiohttp
import feedparser
import pandas as pd
from dagster import MaterializeResult, asset


@asset
def episodes() -> MaterializeResult:
    """
    Get the RSS feed for the podcase and store info on available episodes
    """
    feed_url = "https://www.theguardian.com/news/series/todayinfocus/podcast.xml"
    feed = feedparser.parse(feed_url)
    episodes = feed["entries"]
    os.makedirs("data", exist_ok=True)
    with open("data/episodes.json", "w") as f:
        json.dump(episodes, f, indent=4)

    return MaterializeResult(
        metadata={
            "num_episodes": len(episodes),
            "latest_episode": episodes[0]["title"],
        }
    )


async def _download_file(
    session: aiohttp.ClientSession, url: str, local_path: str
) -> None:
    async with session.get(url) as resp:
        if resp.status == 200:
            # Check to see if we already have a download of the same size. Skip
            # this download if we do.
            try:
                stat = os.stat(local_path)
                file_size = stat.st_size
                download_size = int(resp.headers["Content-Length"])
                if file_size == download_size:
                    print(f"Skipping {url}, already downloaded at {local_path}")
                    resp.close()
                    return
            except FileNotFoundError:
                pass
            print(f"Downloading {url} to {local_path}")
            with open(local_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(1024):
                    f.write(chunk)
        else:
            print(f"[{resp.status}] {url}")


@asset(deps=[episodes])
def download_audio() -> None:
    """
    Download all audio files for all available podcast episodes.

    Note: this can time out. We might want to define a job and set a longer
    timeout, per
    https://docs.dagster.io/deployment/run-monitoring#general-run-timeouts
    """
    os.makedirs("data/downloads", exist_ok=True)
    downloads = []
    with open("data/episodes.json", "r") as f:
        episodes = json.load(f)
        for episode in episodes:
            episode_id = episode["id"]
            mp3_links = [
                link for link in episode["links"] if link["type"] == "audio/mpeg"
            ]
            if len(mp3_links) > 0:
                # Links look like this:
                #
                # https://flex.acast.com/audio.guim.co.uk/...
                #
                # Downloading from flex.acast.com results in a 403 error, but
                # downloading from audio.guim.co.uk does not. So let's do that.
                mp3_url = mp3_links[0]["href"].replace("flex.acast.com/", "")

                output = f"data/downloads/{episode_id}.mp3"
                downloads.append((mp3_url, output))

    async def _f():
        # Limit ourselves to 10 simultaneous connections
        conn = aiohttp.TCPConnector(limit=10)
        session = aiohttp.ClientSession(
            connector=conn,
            headers={"Referer": "https://flex.acast.com/"},
        )
        await asyncio.gather(
            *[_download_file(session, url, path) for (url, path) in downloads]
        )
        await session.close()

    asyncio.run(_f())


@asset(deps=[episodes])
def most_frequent_words() -> None:
    stopwords = [
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "but",
        "for",
        "from",
        "has",
        "his",
        "in",
        "is",
        "it",
        "of",
        "on",
        "reports",
        "the",
        "their",
        "this",
        "to",
        "what",
        "with",
    ]

    episodes = pd.read_json("data/episodes.json")
    word_counts = {}
    for raw_summary in episodes["summary"]:
        summary = raw_summary.lower()
        summary = re.sub(r"help support our.+$", "", summary)
        print(summary)
        for word in summary.split():
            cleaned_word = word.strip(".,-!?:;()[]'\"-")
            if cleaned_word not in stopwords and len(cleaned_word) > 0:
                word_counts[cleaned_word] = word_counts.get(cleaned_word, 0) + 1

    # Get the top 25 most frequent words
    top_words = {
        pair[0]: pair[1]
        for pair in sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:25]
    }

    with open("data/most_frequent_words.json", "w") as f:
        json.dump(top_words, f)
