import asyncio
import json
import os

import aiohttp
import feedparser
from dagster import asset


@asset
def episodes() -> None:
    feed_url = "https://www.theguardian.com/news/series/todayinfocus/podcast.xml"
    episodes = feedparser.parse(feed_url)
    os.makedirs("data", exist_ok=True)
    with open("data/episodes.json", "w") as f:
        json.dump(episodes["entries"], f, indent=4)


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
                mp3_url = mp3_links[0]["href"].replace("flex.acast.com/", "")
                output = f"data/downloads/{episode_id}.mp3"
                downloads.append((mp3_url, output))

    async def _f():
        session = aiohttp.ClientSession(headers={"Referer": "https://flex.acast.com/"})
        print(session.headers)
        await asyncio.gather(
            *[_download_file(session, url, path) for (url, path) in downloads]
        )
        await session.close()

    asyncio.run(_f())
