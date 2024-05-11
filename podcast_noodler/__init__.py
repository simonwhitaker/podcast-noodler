from dagster import Definitions, define_asset_job, load_assets_from_modules

from . import assets

all_assets = load_assets_from_modules([assets])

all_assets_jobs = define_asset_job(name="all_assets_job")

nlp_assets_jobs = define_asset_job(
    name="nlp_assets_job",
    selection=[
        "episodes",
        "most_frequent_tags",
        "most_frequent_summary_words",
    ],
)

defs = Definitions(
    assets=all_assets,
    jobs=[all_assets_jobs, nlp_assets_jobs],
)
