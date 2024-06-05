from dagster import Definitions, define_asset_job, load_assets_from_modules

from . import assets
from .jobs import audio_files_update_job

all_assets = load_assets_from_modules([assets])

all_assets_jobs = define_asset_job(name="all_assets_job")


defs = Definitions(
    assets=all_assets,
    jobs=[all_assets_jobs, audio_files_update_job],
)
