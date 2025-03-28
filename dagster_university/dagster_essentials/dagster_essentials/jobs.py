import dagster as dg

#This defines an asset that we will later exclude
trips_by_week = dg.AssetSelection.assets(["trips_by_week"])

# This job will run all assets except for the trips_by_week asset
trip_update_job = dg.define_asset_job(
    name="trip_update_job",
    selection=dg.AssetSelection.all() - trips_by_week
)

weekly_update_job = dg.define_asset_job(
    name="weekly_update_job",
    selection=trips_by_week
)

