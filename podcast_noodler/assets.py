import asyncio
import json
import os
import re

import aiohttp
import feedparser
import pandas as pd
from dagster import MaterializeResult, MetadataValue, asset

# Note: before using nltk functions, download the local data:
#
#   poetry run python scripts/download-nltk.py
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

from podcast_noodler.utils import download_file

lemmatizer = WordNetLemmatizer()


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
            *[download_file(session, url, path) for (url, path) in downloads]
        )
        await session.close()

    asyncio.run(_f())


@asset(deps=[episodes])
def most_frequent_summary_words() -> MaterializeResult:
    """
    Determines the most commonly-occurring words in the summaries of all the
    episodes in the feed, excluding stopwords.
    """
    episodes = pd.read_json("data/episodes.json")
    word_counts = {}

    for raw_summary in episodes["summary"]:
        summary = raw_summary.lower()
        summary = re.sub(r"help support our.+$", "", summary)
        summary = re.sub(r"[^a-zA-Z]", " ", summary)
        words = summary.split()
        words = [word for word in words if word not in stopwords.words("english")]
        words = [lemmatizer.lemmatize(word) for word in words]

        for word in words:
            word_counts[word] = word_counts.get(word, 0) + 1

    # Get the top 25 most frequent words
    top_words = [
        {"word": word, "count": count}
        for (word, count) in sorted(
            word_counts.items(), key=lambda x: x[1], reverse=True
        )[:25]
    ]
    df = pd.DataFrame(top_words)
    df.to_csv("data/most_frequent_summary_words.csv")

    return MaterializeResult(
        metadata={
            "top_summary_words": MetadataValue.md(df.to_markdown()),
        }
    )

