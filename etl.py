import os
from datetime import datetime, timezone

import pandas as pd
import requests
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


def extract_data() -> list:
    """Extract data from REST API."""
    url = "https://jsonplaceholder.typicode.com/posts"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def transform_data(raw_data: list) -> pd.DataFrame:
    """Aggregate posts by userId — count posts and average title length."""
    df = pd.DataFrame(raw_data)
    result = df.groupby("userId").agg(
        post_count=("id", "count"),
        avg_title_length=("title", lambda x: round(x.str.len().mean(), 2))
    ).reset_index()
    return result


def load_data(data: pd.DataFrame) -> str:
    """Load transformed data to Azure Blob Storage as CSV."""
    account_url = os.environ["AzureWebJobsStorage__blobServiceUri"]
    container_name = "etl-output"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    blob_name = f"aggregated_posts_{timestamp}.csv"

    credential = DefaultAzureCredential()
    blob_service = BlobServiceClient(account_url, credential=credential)

    # Create container if it doesn't exist
    container_client = blob_service.get_container_client(container_name)
    if not container_client.exists():
        container_client.create_container()

    # Upload CSV
    csv_content = data.to_csv(index=False)
    blob_client = blob_service.get_blob_client(container_name, blob_name)
    blob_client.upload_blob(csv_content)

    return blob_name
