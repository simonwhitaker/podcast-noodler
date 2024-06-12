from dagster import Definitions, define_asset_job, load_assets_from_modules

from .assets import episodes, stats
from .jobs import audio_files_update_job
from .resources import openai_resource

episode_assets = load_assets_from_modules([episodes], group_name="episodes")
stats_assets = load_assets_from_modules([stats], group_name="stats")

all_assets_jobs = define_asset_job(name="all_assets_job")


defs = Definitions(
    assets=[*episode_assets, *stats_assets],
    jobs=[all_assets_jobs, audio_files_update_job],
    resources={"openai": openai_resource},
)
