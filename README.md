# ETL Pipeline

![Python](https://img.shields.io/badge/python-3.8%2B-blue)
![Airflow](https://img.shields.io/badge/apache--airflow-2.10-orange)
![License](https://img.shields.io/badge/license-MIT-green)

## Requirements

- Python >=3.8
- Apache Airflow 2.10
- Google Cloud Platform account with required services enabled

## Overview

This project implements an ETL (Extract, Transform, Load) pipeline using Apache Airflow to process data from the DummyJSON API. The pipeline extracts user, product, and cart data, transforms it according to specified schemas, and loads it into Google Cloud Storage and BigQuery for analysis.

## Project Structure

```
project_root/
├── dags/
│   └── etl_dag.py
├── schemas/
│   ├── users.py
│   ├── products.py
│   └── carts.py
├── scripts/
│   ├── helpers.py
│   └── bg_sql_scripts.py
├── config/
│   └── config.py
├── tests/
│   └── test_schemas.py
├── requirements.txt
└── README.md
```

## Installation

### Local Development Setup

1. Clone the repository:

```bash
git clone <repository-url>
```

2. Create and activate virtual environment:

```bash
python -m venv venv

# Linux/macOS
source venv/bin/activate

# Windows
.\venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

## Pipeline Components

### 1. Data Extraction

- Fetches data from DummyJSON API endpoints
- Handles pagination
- Stores raw data locally
- Implements error handling and retries

### 2. Data Transformation

- Validates and cleans data using Pydantic models
- Flattens nested structures
- Filters products based on price threshold (<50)
- Calculates cart values
- Performs data quality checks

### 3. Data Loading

- Uploads raw and cleaned data
- Creates BigQuery tables
- Loads transformed data into BigQuery
- Validates data loads

### 4. Data Analysis

- User purchase summary query
- Category sales analysis query
- Detailed cart information query

## Configuration

### Environment Variables

Create a `.env` file with:

```
BUCKET_NAME=
BUCKET_PATH=
BIGQUERY_PROJECT_DATASET=
BASE_FILE_PATH=
GCP_CONNECTION_ID=
```

### GCP Setup

Required permissions for GCP service account:

- Storage Object Admin
- BigQuery Data Editor
- BigQuery Job User

Place your GCP service account key in `config/gcp.json`

### Testing

Run tests using pytest:

```bash
pytest tests/
```

## Usage

1. Ensure all configuration is set up correctly
2. Start Airflow webserver and scheduler:

```bash
airflow webserver
airflow scheduler
```

3. Access Airflow UI at <http://localhost:8080>
4. Trigger the DAG named `dummy_pipeline` through the Airflow UI
