import json
from typing import Any, Dict, List

import requests
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from requests.exceptions import HTTPError

from config.config import CONFIG


def fetch_api_data(path: str, limit: int = 250, skip: int = 0):
    """
    Fetches data the dummy API and saves the data to a JSON file.
    """
    all_data = []
    while True:
        url = f"{CONFIG.BASE_URL}/{path}?limit={limit}&skip={skip}"
        response = requests.get(url, timeout=30)
        try:
            if response.status_code == 200:
                data = response.json()
                if path in data:
                    all_data.extend(data[path])
                if len(data) < limit:
                    break
                skip += limit

            else:
                print(f"Failed to extract data from {path}")
                break
        except HTTPError as http_err:
            print(f"A Connection error occurred: {http_err}")
            return None
        except Exception as error:
            print(f"An error occurred: {error}")
            return None

    if all_data:
        structured_data = {path: all_data}
        with open(f"{CONFIG.BASE_FILE_PATH}/raw/{path}.json", "w") as f:
            json.dump(structured_data, f)
        return all_data

    return None


def save_to_json(data: List[Dict[str, Any]], filename: str) -> None:
    """
    Save transformed data to a JSON file
    """
    try:
        full_path = f"{CONFIG.BASE_FILE_PATH}/cleaned/{filename}"
        with open(full_path, "w") as f:
            for item in data:
                f.write(json.dumps(item) + "\n")
        print(f"Successfully saved data to {full_path}")
    except Exception as e:
        print(f"Error saving data to {full_path}: {str(e)}")
        raise


def load_to_gcs(
    task_id: str,
    folder_type: str
):
    """
    Helper function to create an operator that loads files from a local filesystem to Google Cloud Storage (GCS).
    """
    return LocalFilesystemToGCSOperator(
        task_id=task_id,
        src=f"{CONFIG.BASE_FILE_PATH}/{folder_type}/*",
        dst=f"{CONFIG.GCS_BUCKET_PATH}/{folder_type}/",
        bucket=CONFIG.GCS_BUCKET_NAME,
        gcp_conn_id=CONFIG.GCP_CONN_ID,
        mime_type="application/json"
    )


def load_to_bigquery(task_id: str, source_path: str, table_name: str):
    """
    Helper function to create an operator that loads files from Google Cloud Storage (GCS) to BigQuery.
    """
    full_destination = f"{CONFIG.BIGQUERY_PROJECT_DATASET}.{table_name}"
    return GCSToBigQueryOperator(
        task_id=task_id,
        bucket=CONFIG.GCS_BUCKET_NAME,
        source_objects=f"{CONFIG.GCS_BUCKET_PATH}/{source_path}",
        destination_project_dataset_table=full_destination,
        autodetect=True,
        gcp_conn_id=CONFIG.GCP_CONN_ID,
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND"
    )


def run_big_query_queries(task_id, query):
    """
    Helper function to create an operator that creates a BigQuery job to run a query.
    """
    return BigQueryInsertJobOperator(
        task_id=task_id,
        configuration={
            "query": {
                "query": query,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": CONFIG.BIGQUERY_PROJECT_DATASET.split(".")[0],
                    "datasetId": CONFIG.BIGQUERY_PROJECT_DATASET.split(".")[1],
                    "tableId": f"table_{task_id}"
                },
                "writeDisposition": "WRITE_APPEND"
            }
        },
        gcp_conn_id=CONFIG.GCP_CONN_ID,
        location="US"
    )
