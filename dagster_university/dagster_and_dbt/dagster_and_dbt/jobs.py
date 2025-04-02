import dagster as dg
from .assets.dbt import dbt_analytics
from dagster_dbt import build_dbt_asset_selection

from .partitions import monthly_partition, weekly_partition

trips_by_week = dg.AssetSelection.assets("trips_by_week")
adhoc_request = dg.AssetSelection.assets("adhoc_request")
# This is how we can select individual assets in the DBT project
#If for whatever reaso we want to exclude stg_trinps and all downstream DBT models
dbt_trips_selection = build_dbt_asset_selection([dbt_analytics], "stg_trips").downstream()


trip_update_job = dg.define_asset_job(
    name="trip_update_job",
    partitions_def=monthly_partition,
    selection=dg.AssetSelection.all() - trips_by_week - adhoc_request - dbt_trips_selection,
)

weekly_update_job = dg.define_asset_job(
    name="weekly_update_job", partitions_def=weekly_partition, selection=trips_by_week
)

adhoc_request_job = dg.define_asset_job(
    name="adhoc_request_job", selection=adhoc_request
)
