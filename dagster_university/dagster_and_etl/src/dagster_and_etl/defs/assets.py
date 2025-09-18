# src/dagster_and_etl/defs/assets.py

import csv
import datetime
import os
from io import StringIO
from pathlib import Path

import dagster as dg
import dlt
import pandas as pd
import requests
from azure.storage.blob import BlobServiceClient
from dagster_duckdb import DuckDBResource
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData
from dagster_and_etl.defs.resources import NASAResource
from pydantic import field_validator

# Partitions

partitions_def = dg.DailyPartitionsDefinition(
    start_date="2018-01-21",
    end_date="2018-01-24",
)

nasa_partitions_def = dg.DailyPartitionsDefinition(
    start_date="2024-04-01",
)




#Configs
class IngestionFileConfig(dg.Config):
    path: str = "2018-01-22.csv"

class IngestionFileAzureConfig(dg.Config):
    connection_string: str = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
class NasaDate(dg.Config):
    date: str

    @field_validator("date")
    @classmethod
    def validate_date_format(cls, v):
        try:
            datetime.datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            raise ValueError("event_date must be in 'YYYY-MM-DD' format")
        return v



def _load_dlt_secrets() -> dict:
    """Return secrets from .dlt/secrets.toml if available."""
    secrets_path = Path(__file__).resolve().parents[3] / ".dlt" / "secrets.toml"
    if not secrets_path.exists():
        return {}

    try:
        import tomllib  # Python >=3.11
    except ModuleNotFoundError:  # pragma: no cover - fallback for <3.11
        import tomli as tomllib  # type: ignore[no-redef]

    with secrets_path.open("rb") as handle:
        return tomllib.load(handle)



#Assets
@dg.asset()
def import_file(context: dg.AssetExecutionContext, config: IngestionFileConfig) -> str:
    file_path = (
        Path(__file__).absolute().parent / f"../../../data/source/{config.path}"
    )
    return str(file_path.resolve())


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
        

## NASA asset 

@dg.asset(
    kinds={"nasa"},
    partitions_def=nasa_partitions_def,
)
def asteroids_partition(
    context: dg.AssetExecutionContext,
    nasa: NASAResource,
) -> list[dict]:
    anchor_date = datetime.datetime.strptime(context.partition_key, "%Y-%m-%d")
    start_date = (anchor_date - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    return nasa.get_near_earth_asteroids(
        start_date=start_date,
        end_date=context.partition_key,
    )

# src/dagster_and_etl/defs/assets.py
@dg.asset(
    partitions_def=nasa_partitions_def,
)
def asteroids_dataframe(
    context: dg.AssetExecutionContext,
    asteroids_partition: list[dict],
) -> pd.DataFrame:
    # Only load specific fields
    fields = [
        "id",
        "name",
        "absolute_magnitude_h",
        "is_potentially_hazardous_asteroid",
    ]

    # Extract only the required fields from the asteroids data
    filtered_data = [
        {key: row[key] for key in fields if key in row} 
        for row in asteroids_partition
    ]
    
    # Convert to DataFrame
    df = pd.DataFrame(filtered_data)

    #Add column 'date' equal to context.partition_key

    df['date'] = context.partition_key
    
    return df

@dg.asset(
    kinds={"duckdb"},
    partitions_def=nasa_partitions_def,
)
def duckdb_table_asteroids(
    context: dg.AssetExecutionContext,
    database: DuckDBResource,
    asteroids_dataframe: pd.DataFrame,
) -> None:
    table_name = "raw_asteroid_data"
    with database.get_connection() as conn:
        table_query = f"""
            create table if not exists {table_name} (
                id varchar(10),
                name varchar(100),
                absolute_magnitude_h float,
                is_potentially_hazardous_asteroid boolean,
                date date
            ) 
        """
        conn.execute(table_query)
        conn.append(table_name, asteroids_dataframe)



    # DLT data load tool assets 

    # Dagster DLT translator
class CustomDagsterDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        default_spec = super().get_asset_spec(data)
        return default_spec.replace_attributes(
            deps=[dg.AssetKey("import_file")],
        )

    # DLT source 

#CSV File
@dlt.source
def csv_source(file_path: str):
    @dlt.resource(name="csv_data", write_disposition="merge")
    def load_csv():
        with open(file_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            # Yield each row individually for DLT to process
            for row in reader:
                yield row

    return load_csv()  # Call the function to return the resource

# NASA API call

@dg.asset(
    kinds={"duckdb", "dlt", "nasa"},
    partitions_def=nasa_partitions_def,
    automation_condition=dg.AutomationCondition.on_cron("@daily"),
)
def dlt_nasa_partition(context: dg.AssetExecutionContext):
    anchor_date = datetime.datetime.strptime(context.partition_key, "%Y-%m-%d")
    start_date = (anchor_date - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    @dlt.source
    def nasa_neo_source():
        @dlt.resource
        def load_neo_data():
            url = "https://api.nasa.gov/neo/rest/v1/feed"
            params = {
                "start_date": start_date,
                "end_date": context.partition_key,
                "api_key": os.getenv("NASA_API_KEY"),
            }

            response = requests.get(url, params=params)
            response.raise_for_status()

            data = response.json()

            for neo in data["near_earth_objects"][context.partition_key]:
                neo_data = {
                    "id": neo["id"],
                    "name": neo["name"],
                    "absolute_magnitude_h": neo["absolute_magnitude_h"],
                    "is_potentially_hazardous": neo[
                        "is_potentially_hazardous_asteroid"
                    ],
                }

                yield neo_data

        return load_neo_data()

    pipeline = dlt.pipeline(
        pipeline_name="nasa_neo_pipeline",
        destination="duckdb",
        dataset_name="nasa_neo",
    )

    load_info = pipeline.run(nasa_neo_source())

    return load_info


@dlt.source
def azure_blob_csv_source(
    connection_string: str,
    container_name: str,
    blob_name: str,
    resource_name: str,
):
    @dlt.source(name=f"{resource_name}_source")
    def _azure_blob_source():
        @dlt.resource(name=resource_name, write_disposition="replace")
        def _csv_rows():
            blob_service_client = BlobServiceClient.from_connection_string(
                connection_string
            )
            blob_client = blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_name,
            )

            csv_bytes = blob_client.download_blob().readall()
            reader = csv.DictReader(StringIO(csv_bytes.decode("utf-8")))
            for row in reader:
                yield row

        return _csv_rows()

    return _azure_blob_source()


def _resolve_azure_connection_string(secrets: dict, secret_key: str) -> str:
    sources = secrets.get("sources", {})
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING") or (
        sources.get(secret_key, {})
        .get("credentials", {})
        .get("connection_string")
        or sources.get("azure_credit_cards", {})
        .get("credentials", {})
        .get("connection_string")
    )

    if not connection_string:
        raise RuntimeError(
            "Missing Azure credentials. Set AZURE_STORAGE_CONNECTION_STRING in the "
            "environment or add it under [sources.azure_credit_cards.credentials] in "
            ".dlt/secrets.toml before materializing the Azure DLT assets."
        )

    return connection_string


def _ensure_motherduck_credentials(secrets: dict) -> None:
    motherduck_credentials = (
        secrets.get("destination", {})
        .get("motherduck", {})
        .get("credentials", {})
    )

    if not motherduck_credentials:
        raise RuntimeError(
            "Missing MotherDuck credentials. Add [destination.motherduck.credentials] "
            "with database and password/token values in .dlt/secrets.toml."
        )


def _run_azure_blob_to_motherduck(
    connection_string: str,
    pipeline_name: str,
    dataset_name: str,
    container_name: str,
    blob_name: str,
    resource_name: str,
):
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="motherduck",
        dataset_name=dataset_name,
    )

    return pipeline.run(
        azure_blob_csv_source(
            connection_string=connection_string,
            container_name=container_name,
            blob_name=blob_name,
            resource_name=resource_name,
        )
    )


@dg.asset(
    kinds={"azure", "motherduck", "dlt"},
    description="Loads credit card data from Azure Blob Storage into MotherDuck via DLT.",
)
def dlt_azure_credit_cards(context: dg.AssetExecutionContext):
    secrets = _load_dlt_secrets()
    connection_string = _resolve_azure_connection_string(secrets, "azure_credit_cards")
    _ensure_motherduck_credentials(secrets)

    return _run_azure_blob_to_motherduck(
        connection_string=connection_string,
        pipeline_name="azure_credit_cards_motherduck",
        dataset_name="azure_credit_cards",
        container_name="creditcards",
        blob_name="credit_cards.csv",
        resource_name="credit_cards",
    )


@dg.asset(
    kinds={"azure", "motherduck", "dlt"},
    description="Loads interchange fee data from Azure Blob Storage into MotherDuck via DLT.",
)
def dlt_azure_interchange_fees(context: dg.AssetExecutionContext):
    secrets = _load_dlt_secrets()
    connection_string = _resolve_azure_connection_string(secrets, "azure_interchange_fees")
    _ensure_motherduck_credentials(secrets)

    return _run_azure_blob_to_motherduck(
        connection_string=connection_string,
        pipeline_name="azure_interchange_fees_motherduck",
        dataset_name="azure_interchange_fees",
        container_name="interchangefees",
        blob_name="interchange_fees.csv",
        resource_name="interchange_fees",
    )


@dg.asset(
    kinds={"azure", "motherduck", "dlt"},
    description="Loads merchant service fee data from Azure Blob Storage into MotherDuck via DLT.",
)
def dlt_azure_merchant_service_fees(context: dg.AssetExecutionContext):
    secrets = _load_dlt_secrets()
    connection_string = _resolve_azure_connection_string(
        secrets, "azure_merchant_service_fees"
    )
    _ensure_motherduck_credentials(secrets)

    return _run_azure_blob_to_motherduck(
        connection_string=connection_string,
        pipeline_name="azure_merchant_service_fees_motherduck",
        dataset_name="azure_merchant_service_fees",
        container_name="merchantservicefees",
        blob_name="merchant_service_fees.csv",
        resource_name="merchant_service_fees",
    )


@dlt_assets(
    dlt_source=csv_source(import_file),  # File path
    dlt_pipeline=dlt.pipeline(
        pipeline_name="csv_pipeline",
        dataset_name="csv_data",
        destination="duckdb",
        progress="log",
    ),
    dagster_dlt_translator=CustomDagsterDltTranslator(),
)
def dlt_csv_assets(
    context: dg.AssetExecutionContext, dlt: DagsterDltResource, import_file: str
):
    yield from dlt.run(context=context, dlt_source=csv_source(import_file))
