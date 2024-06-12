import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import feedparser
import whisper
from dagster import AssetExecutionContext, MaterializeResult, asset

from podcast_noodler.utils import download_file

from ..partitions import weekly_partition
from ..utils import sluggify
from .constants import (
    AUDIO_FILE_PARTITION_FILE_PATH_TEMPLATE,
    EPISODES_METADATA_FILE_PATH,
    TRANSCRIPT_PARTITION_FILE_PATH_TEMPLATE,
)


@asset
def episode_metadata() -> MaterializeResult:
    """
    Get the RSS feed for the podcase and store info on available episodes
    """

    # Get the feed and parse it
    feed_url = "https://www.theguardian.com/news/series/todayinfocus/podcast.xml"
    feed = feedparser.parse(feed_url)
    episodes = feed["entries"]

    # Store the feed in JSON format
    metadata_path = Path(EPISODES_METADATA_FILE_PATH)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    with metadata_path.open("w") as f:
        json.dump(episodes, f, indent=4)

    return MaterializeResult(
        metadata={
            "num_episodes": len(episodes),
            "latest_episode": episodes[0]["title"],
        }
    )


@asset(partitions_def=weekly_partition, deps=[episode_metadata])
def audio_files(context: AssetExecutionContext) -> None:
    """
    The audio files for available podcast episodes.
    """

    partition_key = context.partition_key  # YYYY-MM-DD
    partition_time_window = context.partition_time_window
    partition_dir = Path(AUDIO_FILE_PARTITION_FILE_PATH_TEMPLATE.format(partition_key))
    partition_dir.mkdir(parents=True, exist_ok=True)

    downloads = []
    with open(EPISODES_METADATA_FILE_PATH, "r") as f:
        episodes = json.load(f)
        for episode in episodes:
            date_elements = episode["published_parsed"][
                0:6
            ]  # [year, month, day, hour, min, sec]
            published_timestamp = datetime(*date_elements, tzinfo=timezone.utc)
            if (
                published_timestamp > partition_time_window.end
                or published_timestamp < partition_time_window.start
            ):
                continue

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

                episode_filename = f"{date_elements[0]}-{date_elements[1]:02d}-{date_elements[2]:02d}-{sluggify(episode['title'])}.mp3"

                output = f"{partition_dir}/{episode_filename}"
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


@asset(deps=[audio_files], partitions_def=weekly_partition)
def transcripts(context: AssetExecutionContext):
    partition_key = context.partition_key
    audio_file_dir = Path(AUDIO_FILE_PARTITION_FILE_PATH_TEMPLATE.format(partition_key))
    transcript_dir = Path(TRANSCRIPT_PARTITION_FILE_PATH_TEMPLATE.format(partition_key))

    transcript_dir.mkdir(parents=True, exist_ok=True)
    model = whisper.load_model("base")

    for mp3_path in audio_file_dir.iterdir():
        txt_filename = mp3_path.name.replace(".mp3", ".txt")
        txt_path = transcript_dir / txt_filename

        if txt_path.exists():
            print(f"{txt_path} already exists, skipping")
        else:
            result = model.transcribe(str(mp3_path))
            with open(txt_path, "w") as f:
                f.write(str(result["text"]))
