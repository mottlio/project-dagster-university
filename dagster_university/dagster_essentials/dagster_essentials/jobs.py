import dagster as dg
from .partitions import monthly_partition, weekly_partition

#This defines an asset that we will later exclude
trips_by_week = dg.AssetSelection.assets(["trips_by_week"])
adhoc_request = dg.AssetSelection.assets(["adhoc_request"])
atp_matches = dg.AssetSelection.assets(["atp_matches_dataset"])

# This job will run the adhoc_request asset
adhoc_request_job = dg.define_asset_job(
    name="adhoc_request_job",
    selection=adhoc_request,
)

atp_matches_job = dg.define_asset_job(
    name="atp_matches_job",
    selection = atp_matches,
    # This job will run the atp_matches_dataset asset
    partitions_def = monthly_partition # partitions added here
)


# This job will run all assets except for the trips_by_week asset
trip_update_job = dg.define_asset_job(
    name="trip_update_job",
    # This job will run all assets except for the trips_by_week asset
    selection=dg.AssetSelection.all() - trips_by_week - adhoc_request,
    partitions_def = monthly_partition # partitions added here
)

weekly_update_job = dg.define_asset_job(
    name="weekly_update_job",
    selection=trips_by_week,
    partitions_def = weekly_partition # partitions added here
)

