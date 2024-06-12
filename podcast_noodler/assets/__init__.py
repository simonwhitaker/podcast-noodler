import asyncio
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import feedparser
import pandas as pd
import whisper
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

# Note: before using nltk functions, download the local data:
#
#   poetry run python scripts/download-nltk.py
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

from podcast_noodler.utils import download_file

from ..partitions import weekly_partition
from ..utils import sluggify
from .constants import (
    AUDIO_FILE_PARTITION_FILE_PATH_TEMPLATE,
    EPISODES_METADATA_FILE_PATH,
    TRANSCRIPT_PARTITION_FILE_PATH_TEMPLATE,
)

lemmatizer = WordNetLemmatizer()


@asset
def episode_metadata() -> MaterializeResult:
    """
    Get the RSS feed for the podcase and store info on available episodes
    """
    feed_url = "https://www.theguardian.com/news/series/todayinfocus/podcast.xml"
    feed = feedparser.parse(feed_url)
    episodes = feed["entries"]
    os.makedirs("data", exist_ok=True)
    with open(EPISODES_METADATA_FILE_PATH, "w") as f:
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
    partition_dir = AUDIO_FILE_PARTITION_FILE_PATH_TEMPLATE.format(partition_key)
    os.makedirs(partition_dir, exist_ok=True)

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
    os.makedirs(transcript_dir, exist_ok=True)
    model = whisper.load_model("base")

    for mp3_filename in os.listdir(audio_file_dir):
        mp3_path = audio_file_dir / mp3_filename
        txt_filename = mp3_filename.replace(".mp3", ".txt")
        txt_path = transcript_dir / txt_filename

        if txt_path.exists():
            print(f"{txt_path} already exists, skipping")
        else:
            result = model.transcribe(str(mp3_path))
            with open(txt_path, "w") as f:
                f.write(str(result["text"]))


@asset(deps=[episode_metadata])
def most_frequent_summary_words() -> MaterializeResult:
    """
    Determines the most commonly-occurring words in the summaries of all the
    episodes in the feed, excluding stopwords.
    """
    episodes = pd.read_json(EPISODES_METADATA_FILE_PATH)
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


@asset(deps=[episode_metadata])
def most_frequent_tags() -> MaterializeResult:
    tag_counts = {}
    with open(EPISODES_METADATA_FILE_PATH) as f:
        episodes = json.load(f)
        for episode in episodes:
            tags = [t.get("term") for t in episode["tags"]]
            for tag in tags:
                tag_counts[tag] = tag_counts.get(tag, 0) + 1

    top_tags = [
        {"term": term, "count": count}
        for (term, count) in sorted(
            tag_counts.items(), key=lambda x: x[1], reverse=True
        )[:25]
    ]
    df = pd.DataFrame(top_tags)
    df.to_csv("data/most_frequent_tags.csv")

    return MaterializeResult(
        metadata={
            "top_tags": MetadataValue.md(df.to_markdown()),
        }
    )
