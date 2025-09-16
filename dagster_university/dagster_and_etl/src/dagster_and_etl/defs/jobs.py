import dagster as dg
from dagster_and_etl.defs.assets import (
    import_partition_file,
    duckdb_partition_table,
    import_dynamic_partition_file,
    duckdb_dynamic_partition_table,
    partitions_def,
    dynamic_partitions_def,
    import_file_azure_credit_cards,
    duckdb_table_azure_credit_cards,
    import_file_azure_interchange_fees,
    duckdb_table_azure_interchange_fees,
    import_file_azure_merchant_service_fees,
    duckdb_table_azure_merchant_service_fees,
    asteroids,
    asteroids_dataframe,
    duckdb_table_asteroids
)

# Job for static partitioned assets (used by the schedule)
import_partition_job = dg.define_asset_job(
    name="import_partition_job",
    selection=dg.AssetSelection.assets(
        import_partition_file,
        duckdb_partition_table,
    ),
    partitions_def=partitions_def,
)

# Job for dynamic partitioned assets (used by the sensor)
import_dynamic_partition_job = dg.define_asset_job(
    name="import_dynamic_partition_job",
    selection=dg.AssetSelection.assets(
        import_dynamic_partition_file,
        duckdb_dynamic_partition_table,
    ),
    partitions_def=dynamic_partitions_def,
)

# Job to transfer credit card data from Azure Blob Storage to DuckDB
azure_credit_cards_job = dg.define_asset_job(
    name="azure_credit_cards_job",
    selection=dg.AssetSelection.assets(
        import_file_azure_credit_cards,
        duckdb_table_azure_credit_cards,
    ),
)

# Job to transfer interchange fee data from Azure Blob Storage to DuckDB
azure_interchange_fees_job = dg.define_asset_job(
    name="azure_interchange_fees_job",
    selection=dg.AssetSelection.assets(
        import_file_azure_interchange_fees,
        duckdb_table_azure_interchange_fees,
    ),
)

# Job to transfer merchant service fee data from Azure Blob Storage to DuckDB
azure_merchant_service_fees_job = dg.define_asset_job(
    name="azure_merchant_service_fees_job",
    selection=dg.AssetSelection.assets(
        import_file_azure_merchant_service_fees,
        duckdb_table_azure_merchant_service_fees,
    ),
)

# Job to transfer all Azure data to DuckDB
azure_all_data_job = dg.define_asset_job(
    name="azure_all_data_job",
    selection=dg.AssetSelection.assets(
        import_file_azure_credit_cards,
        duckdb_table_azure_credit_cards,
        import_file_azure_interchange_fees,
        duckdb_table_azure_interchange_fees,
        import_file_azure_merchant_service_fees,
        duckdb_table_azure_merchant_service_fees,
    ),
)

# Job to fetch NASA asteroid data and store in DuckDB
nasa_asteroids_job = dg.define_asset_job(
    name="nasa_asteroids_job",
    selection=dg.AssetSelection.assets(
        asteroids,
        asteroids_dataframe,
        duckdb_table_asteroids,
    ),
)