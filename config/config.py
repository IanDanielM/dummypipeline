from os import getenv

from dotenv import load_dotenv

load_dotenv()


class CONFIG:
    BASE_URL = "https://dummyjson.com"
    BASE_FILE_PATH = getenv("BASE_FILE_PATH")  # must be full local path
    GCS_BUCKET_NAME = getenv("BUCKET_NAME")
    GCS_BUCKET_PATH = getenv("BUCKET_PATH")
    GCP_CONN_ID = getenv("GCP_CONNECTION_ID")
    BIGQUERY_PROJECT_DATASET = getenv("BIGQUERY_PROJECT_DATASET")
