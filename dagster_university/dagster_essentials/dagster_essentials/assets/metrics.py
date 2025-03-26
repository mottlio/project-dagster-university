import dagster as dg

import matplotlib.pyplot as plt
import geopandas as gpd
import pandas as pd

from dagster_duckdb import DuckDBResource

from . import constants

@dg.asset(
    deps=["taxi_trips", "taxi_zones"]
)
def manhattan_stats(database: DuckDBResource) -> None:
    query = """
        select
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips,
        from trips
        left join zones on trips.pickup_zone_id = zones.zone_id
        where borough = 'Manhattan' and geometry is not null
        group by zone, borough, geometry
    """

    
    with database.get_connection() as conn:
        trips_by_zone = conn.execute(query).fetch_df()
    
    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())

@dg.asset(
    deps=["manhattan_stats"],
)
def manhattan_map() -> None:
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig, ax = plt.subplots(figsize=(10, 10))
    trips_by_zone.plot(column="num_trips", cmap="plasma", legend=True, ax=ax, edgecolor="black")
    ax.set_title("Number of Trips per Taxi Zone in Manhattan")

    ax.set_xlim([-74.05, -73.90])  # Adjust longitude range
    ax.set_ylim([40.70, 40.82])  # Adjust latitude range
    
    # Save the image
    plt.savefig(constants.MANHATTAN_MAP_FILE_PATH, format="png", bbox_inches="tight")
    plt.close(fig)




@dg.asset(
deps=["taxi_trips"]
)
def trips_by_week(database: DuckDBResource) -> None:
    query = """
        select
            DATE_TRUNC('week', dropoff_datetime) + INTERVAL '6 days' AS week_ending_sunday,
            COUNT(1) as num_trips,
            SUM(passenger_count) as passenger_count,
            SUM(total_amount) as total_amount,
            SUM(trip_distance) as trip_distance
        from trips
        group by week_ending_sunday
    """

    with database.get_connection() as conn:
        trips_by_week = conn.execute(query).fetch_df()

    with open(constants.TRIPS_BY_WEEK_FILE_PATH, "w", encoding="utf-8") as output_file:
        output_file.write(trips_by_week.to_csv(index=False))
