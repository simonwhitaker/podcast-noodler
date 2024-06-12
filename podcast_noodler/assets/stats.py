import json
import re

import pandas as pd
from dagster import MaterializeResult, MetadataValue, asset

# Note: before using nltk functions, download the local data:
#
#   poetry run python scripts/download-nltk.py
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

from .constants import EPISODES_METADATA_FILE_PATH

lemmatizer = WordNetLemmatizer()


@asset(deps=["episode_metadata"])
def most_frequent_summary_words() -> MaterializeResult:
    """
    The most commonly-occurring words in the summaries of all the
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


@asset(deps=["episode_metadata"])
def most_frequent_tags() -> MaterializeResult:
    """
    The most frequent tags in the episode metadata
    """
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
