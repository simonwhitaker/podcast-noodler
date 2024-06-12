import os

data_dir = os.environ.get("DATA_DIR", "data")

EPISODES_METADATA_FILE_PATH = data_dir + "/episodes.json"
AUDIO_FILE_PARTITION_FILE_PATH_TEMPLATE = data_dir + "/downloads/{}"
TRANSCRIPT_PARTITION_FILE_PATH_TEMPLATE = data_dir + "/transcripts/{}"
SUMMARIES_PARTITION_FILE_PATH_TEMPLATE = data_dir + "/summaries/{}"
