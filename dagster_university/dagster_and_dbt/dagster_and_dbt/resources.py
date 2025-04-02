import os

import boto3
import dagster as dg
from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtCliResource

#Import dbt_project
from .project import dbt_project


#DuckDB database resource
database_resource = DuckDBResource(
    database=dg.EnvVar("DUCKDB_DATABASE"),
)

# Smart Open configuration for S3
# This is used to read and write files to S3
# using the smart_open library.
if os.getenv("DAGSTER_ENVIRONMENT") == "prod":
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )
    smart_open_config = {"client": session.client("s3")}
else:
    smart_open_config = {}

# DBT CLI resource
# This is used to run DBT commands from Dagster
# The dbt_project variable is defined in the project module
# and contains the path to the DBT project directory.
dbt_resource = DbtCliResource(
    project_dir=dbt_project,
)

