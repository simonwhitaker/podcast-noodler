from dagster import AssetSelection, define_asset_job

from ..partitions import monthly_partition

audio_files_assets = AssetSelection.assets("episodes", "audio_files")

audio_files_update_job = define_asset_job(
    name="audio_files_update_job",
    selection=audio_files_assets,
    partitions_def=monthly_partition,
)
