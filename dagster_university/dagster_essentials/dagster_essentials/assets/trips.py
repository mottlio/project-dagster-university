import requests
from . import constants
from ..partitions import monthly_partition
import dagster as dg
from dagster_duckdb import DuckDBResource
import pandas as pd


@dg.asset(
        partitions_def=monthly_partition,
        group_name='raw_data'
)
def taxi_trips_file(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
      The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """

    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]

    #month_to_fetch = '2023-03' - Replaced by partition
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)

    num_rows = len(pd.read_parquet(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)))

    return dg.MaterializeResult(
        metadata={
            'Number of records': dg.MetadataValue.int(num_rows)
        }
    )

@dg.asset(
       group_name = "raw_data"
)
def taxi_zones_file() -> dg.MaterializeResult:
    """
      The raw CSV file for the taxi zones dataset. Sourced from the NYC Open Data portal.
    """
    raw_zones = requests.get(
        "https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv"
    )

    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(raw_zones.content)

    num_rows = len(pd.read_csv(constants.TAXI_ZONES_FILE_PATH))
    
    return dg.MaterializeResult(
        metadata={
            'Number of zones ingested': dg.MetadataValue.int(num_rows)
        }
    )


@dg.asset(
    deps=["taxi_trips_file"],
    partitions_def=monthly_partition,
)
def taxi_trips(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:
    """
      The raw taxi trips dataset, loaded into a DuckDB database
    """

    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]

    query = f"""
        create table if not exists trips (
        vendor_id integer, pickup_zone_id integer, dropoff_zone_id integer,
        rate_code_id double, payment_type integer, dropoff_datetime timestamp,
        pickup_datetime timestamp, trip_distance double, passenger_count double,
        total_amount double, partition_date varchar
        );
    
        delete from trips
        where partition_date = '{month_to_fetch}';
        
        insert into trips
        select
            VendorID as vendor_id,
            PULocationID as pickup_zone_id,
            DOLocationID as dropoff_zone_id,
            RatecodeID as rate_code_id,
            payment_type as payment_type,
            tpep_dropoff_datetime as dropoff_datetime,
            tpep_pickup_datetime as pickup_datetime,
            trip_distance as trip_distance,
            passenger_count as passenger_count,
            total_amount as total_amount,
            '{month_to_fetch}' as partition_date
        from '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}'
        """

    with database.get_connection() as conn:
        conn.execute(query)



@dg.asset(
    deps=["taxi_zones_file"]
)
def taxi_zones(database: DuckDBResource) -> None:
    """
      The raw taxi zones dataset, loaded into a DuckDB database
    """
    query = f"""
        create or replace table zones as (
          select
            LocationID as zone_id,
            borough,
            zone,
            the_geom as geometry
          from "data/raw/taxi_zones.csv"
        );
    """

    with database.get_connection() as conn:
        conn.execute(query)


    
