# src/dagster_and_etl/defs/assets.py

import csv
import dagster as dg
from pathlib import Path
import pandas as pd
from io import StringIO
from azure.storage.blob import BlobServiceClient
import os

class IngestionFileConfig(dg.Config):
    path: str = "2018-01-22.csv"


class IngestionFileAzureConfig(dg.Config):
    connection_string: str = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")


@dg.asset()
def import_file(context: dg.AssetExecutionContext, config: IngestionFileConfig) -> str:
    file_path = (
        Path(__file__).absolute().parent / f"../../../data/source/{config.path}"
    )
    return str(file_path.resolve())


# src/dagster_and_etl/defs/assets.py
from dagster_duckdb import DuckDBResource

@dg.asset(
    kinds={"duckdb"},
)
def duckdb_table(
    context: dg.AssetExecutionContext,
    database: DuckDBResource,
    import_file,
):
    table_name = "raw_data"
    with database.get_connection() as conn:
        table_query = f"""
            create table if not exists {table_name} (
                date date,
                share_price float,
                amount float,
                spend float,
                shift float,
                spread float
            ) 
        """
        conn.execute(table_query)
        conn.execute(f"copy {table_name} from '{import_file}';")



@dg.asset_check(
    asset=import_file,
    blocking=True,
    description="Ensure file contains no zero value shares",
)
def not_empty(
    context: dg.AssetCheckExecutionContext,
    import_file,
) -> dg.AssetCheckResult:
    with open(import_file, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        data = (row for row in reader)

        for row in data:
            if float(row["share_price"]) <= 0:
                return dg.AssetCheckResult(
                    passed=False,
                    metadata={"'share' is below 0": row},
                )

    return dg.AssetCheckResult(
        passed=True,
    )

#Partitioned assets

partitions_def = dg.DailyPartitionsDefinition(
    start_date="2018-01-21",
    end_date="2018-01-24",
)


@dg.asset(
    partitions_def=partitions_def,
)
def import_partition_file(context: dg.AssetExecutionContext) -> str:
    file_path = (
        Path(__file__).absolute().parent
        / f"../../../data/source/{context.partition_key}.csv"
    )
    return str(file_path.resolve())

@dg.asset(
    kinds={"duckdb"},
    partitions_def=partitions_def,
)
def duckdb_partition_table(
    context: dg.AssetExecutionContext,
    database: DuckDBResource,
    import_partition_file,
):
    table_name = "raw_partition_data"
    with database.get_connection() as conn:
        table_query = f"""
            create table if not exists {table_name} (
                date date,
                share_price float,
                amount float,
                spend float,
                shift float,
                spread float
            ) 
        """
        conn.execute(table_query)
        conn.execute(
            f"delete from {table_name} where date = '{context.partition_key}';"
        )
        conn.execute(f"copy {table_name} from '{import_partition_file}';")

## Example of dynamic partitions - more complex logic

#Create partition definition - without any logic - it can be whatever we choose

dynamic_partitions_def = dg.DynamicPartitionsDefinition(name="dynamic_partition")

# Create asset that will add partitions to the definition

@dg.asset(
    partitions_def=dynamic_partitions_def,
)
def import_dynamic_partition_file(context: dg.AssetExecutionContext) -> str:
    file_path = (
        Path(__file__).absolute().parent
        / f"../../../data/source/{context.partition_key}.csv"
    )
    return str(file_path.resolve())

# Create a downstream asset that will add partitions to the definition
@dg.asset(
    kinds={"duckdb"},
    partitions_def=dynamic_partitions_def,
)
def duckdb_dynamic_partition_table(
    context: dg.AssetExecutionContext,
    database: DuckDBResource,
    import_dynamic_partition_file,
):
    table_name = "raw_dynamic_partition_data"
    with database.get_connection() as conn:
        table_query = f"""
            create table if not exists {table_name} (
                date date,
                share_price float,
                amount float,
                spend float,
                shift float,
                spread float
            ) 
        """
        conn.execute(table_query)
        conn.execute(
            f"delete from {table_name} where date = '{context.partition_key}';"
        )
        conn.execute(f"copy {table_name} from '{import_dynamic_partition_file}';")

# Asset to transfer credit card data from Azure Blob Storage to DuckDB

@dg.asset(
    kinds={"azure"},
)
def import_file_azure_credit_cards(
    context: dg.AssetExecutionContext,
    config: IngestionFileAzureConfig,
) -> pd.DataFrame:
    blob_service_client = BlobServiceClient.from_connection_string(config.connection_string)
    blob_client = blob_service_client.get_blob_client(
        container = "creditcards",
        blob="credit_cards.csv"
        )
    
    blob_data = blob_client.download_blob().readall()

    #Read csv into a Pandas dataframe
    csv_string = blob_data.decode('utf-8')
    df = pd.read_csv(StringIO(csv_string))
    return df

@dg.asset(
    kinds={"duckdb"},
)
def duckdb_table_azure_credit_cards(
    context: dg.AssetExecutionContext,
    database: DuckDBResource,
    import_file_azure_credit_cards: pd.DataFrame,
):
    table_name = "raw_credit_card_data"
    with database.get_connection() as conn:
        # Create table from DataFrame
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM import_file_azure_credit_cards")

# Asset to transfer interchange fee data from Azure Blob Storage to DuckDB

@dg.asset(
    kinds={"azure"},
)
def import_file_azure_interchange_fees(
    context: dg.AssetExecutionContext,
    config: IngestionFileAzureConfig,
) -> pd.DataFrame:
    blob_service_client = BlobServiceClient.from_connection_string(config.connection_string)
    blob_client = blob_service_client.get_blob_client(
        container = "interchangefees",
        blob="interchange_fees.csv"
    )
    blob_data = blob_client.download_blob().readall()

    #Read csv into a Pandas dataframe
    csv_string = blob_data.decode('utf-8')
    df = pd.read_csv(StringIO(csv_string))
    return df

@dg.asset(
    kinds={"duckdb"},
)
def duckdb_table_azure_interchange_fees(
    context: dg.AssetExecutionContext,
    database: DuckDBResource,
    import_file_azure_interchange_fees: pd.DataFrame,
):
    table_name = "raw_interchange_fee_data"
    with database.get_connection() as conn:
        # Create table from DataFrame
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM import_file_azure_interchange_fees")

# Asset to transfer merchant service fee data from Azure Blob Storage to DuckDB

@dg.asset(
    kinds={"azure"},
)
def import_file_azure_merchant_service_fees(
    context: dg.AssetExecutionContext,
    config: IngestionFileAzureConfig,
) -> pd.DataFrame:
    blob_service_client = BlobServiceClient.from_connection_string(config.connection_string)
    blob_client = blob_service_client.get_blob_client(
        container = "merchantservicefees",
        blob="merchant_service_fees.csv"
    )
    blob_data = blob_client.download_blob().readall()

    #Read csv into a Pandas dataframe
    csv_string = blob_data.decode('utf-8')
    df = pd.read_csv(StringIO(csv_string))
    return df

@dg.asset(
    kinds={"duckdb"},
)
def duckdb_table_azure_merchant_service_fees(
    context: dg.AssetExecutionContext,
    database: DuckDBResource,
    import_file_azure_merchant_service_fees: pd.DataFrame,
):
    table_name = "raw_merchant_service_fee_data"
    with database.get_connection() as conn:
        # Create table from DataFrame
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM import_file_azure_merchant_service_fees")
        

