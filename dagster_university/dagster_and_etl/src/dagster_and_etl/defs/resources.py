# src/dagster_and_etl/defs/resources.py
import os
from io import BytesIO
from dotenv import load_dotenv
import dagster as dg
import requests
from dagster_duckdb import DuckDBResource

# Load environment variables
load_dotenv()

class AzureBlobStorageResource(dg.ConfigurableResource):
    """Resource for accessing files from Azure Blob Storage."""
    
    connection_string: str = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")

class NASAResource(dg.ConfigurableResource):
    api_key: str

    def get_near_earth_asteroids(self, start_date: str, end_date: str):
        url = "https://api.nasa.gov/neo/rest/v1/feed"
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "api_key": self.api_key,
        }

        resp = requests.get(url, params=params)
        return resp.json()["near_earth_objects"][start_date]

    

@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "nasa": NASAResource(
                api_key=dg.EnvVar("NASA_API_KEY"),
            ),
            "database": DuckDBResource(
                database="data/staging/data.duckdb",
            ),
            "azure_blob_storage": AzureBlobStorageResource()
        }
    )
