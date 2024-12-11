from datetime import datetime

from airflow.decorators import dag, task, task_group
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

from config.config import CONFIG
from schemas.carts import CartSchema
from schemas.products import ProductSchema
from schemas.users import UserSchema
from scripts.bg_sql_scripts import (CART_DETAILS_QUERY, CATEGORY_SUMMARY_QUERY,
                                    USER_SUMMARY_QUERY)
from scripts.helpers import (fetch_api_data, load_to_bigquery, load_to_gcs,
                             run_big_query_queries, save_to_json)


@dag(
    start_date=datetime(2024, 12, 1),
    description="DAG for scraping and repricing caliper products",
    schedule_interval=None,
    catchup=False,
    tags=["ETL"],
)
def dummy_pipeline():
    @task_group(group_id="extract", tooltip="Extract data from API endpoints")
    def extract():
        """
        Task group to extract data from API endpoints.
        This task group creates a task for each API endpoint to extract data
        based on the path.

        Tasks:
            extract_{path}: Fetches data for the specified path.
        """
        path_list = ["users", "products", "carts"]
        extract_tasks = []
        for path in path_list:
            @task(task_id=f"extract_{path}")
            def extract_data_task(path: str):
                all_data = fetch_api_data(path, limit=250, skip=0)  # Fetch data from API
                return {path: all_data}
            extract_tasks.append(extract_data_task(path))
        return extract_tasks

    @task()
    def aggregate_data(*response_data):
        """
        Aggregates multiple dictionaries into a single dictionary.
        """
        aggregated_result = {}
        for result in response_data:
            aggregated_result.update(result)
        return aggregated_result

    @task_group(group_id="transform", tooltip="Transform extracted data")
    def transform():
        """
        Task group to clean and transform extracted data.

        Tasks:
            transform_user_data: cleans and transforms user data based on the UserSchema.
            transform_product_data: cleans and transforms product data based on the ProductSchema.
            transform_cart_data: cleans and transforms cart data based on the CartSchema.
        """
        @task()
        def transform_user_data(response_data: dict):
            transformed_users = []
            for user_data in response_data["users"]:
                try:
                    user = UserSchema(**user_data)  # Create a UserSchema instance
                    transformed_users.append(user.flatten_address())  # flattens the address
                except Exception as e:
                    print(f"An error occurred: {e}")
                    continue

            # Save the cleaned user data to a JSON file
            save_to_json(data=transformed_users, filename="users_cleaned.json")
            return "users data successfully transformed"

        @task()
        def transform_product_data(response_data: dict):
            transformed_products = []
            for product_data in response_data["products"]:
                try:
                    product = ProductSchema(**product_data)  # Create a ProductSchema instance
                    cleaned_product = product.get_cleaned_data()
                    if cleaned_product:
                        transformed_products.append(cleaned_product)
                except Exception as e:
                    print(f"An error occurred: {e}")
                    continue

            # Save the cleaned product data to a JSON file
            save_to_json(data=transformed_products, filename="products_cleaned.json")
            return "products data successfully transformed"

        @task()
        def transform_cart_data(response_data: dict):
            transformed_carts = []
            for cart_data in response_data["carts"]:
                try:
                    cart = CartSchema(**cart_data)
                    transformed_carts.extend(cart.flatten_products())
                except Exception as e:
                    print(f"An error occurred: {e}")
                    continue

            # Save the cleaned cart data to a JSON file
            save_to_json(data=transformed_carts, filename="carts_cleaned.json")
            return "carts data successfully transformed"

        cleaned_users = transform_user_data(aggregated_data)
        cleaned_products = transform_product_data(aggregated_data)
        cleaned_carts = transform_cart_data(aggregated_data)

    @task_group(group_id="load", tooltip="Load data to GCS")
    def load():
        """
        Task group to load data to Google Cloud Storage (GCS).

        Tasks:
            upload_raw_datasets_to_gcs: uploads raw datasets to GCS.
            upload_clean_datasets_to_gcs: uploads cleaned datasets to GCS.
        """
        upload_raw_datasets_to_gcs = load_to_gcs(
            task_id="upload_raw_datasets_to_gcs",
            folder_type="raw"
        )

        upload_clean_datasets_to_gcs = load_to_gcs(
            task_id="upload_cleaned_datasets_to_gcs",
            folder_type="cleaned"
        )

    # Create an empty dataset in BigQuery
    create_dummy_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dummy_dataset",
        dataset_id="dummyjson",
        gcp_conn_id=CONFIG.GCP_CONN_ID
    )

    @task_group(group_id="load_to_bigquery", tooltip="Load cleaned data to BigQuery")
    def load_data():
        """
        Task group to load cleaned data to BigQuery.

        Tasks:
            load_users_to_bigquery: Loads cleaned user data into the users table.
            load_carts_to_bigquery: Loads cleaned cart data into the carts table.
            load_products_to_bigquery: Loads cleaned product data into the products table.
        """
        load_users_to_bigquery = load_to_bigquery(
            task_id="load_users_to_bigquery",
            source_path="cleaned/users_cleaned.json",
            table_name="users",
        )

        load_carts_to_bigquery = load_to_bigquery(
            task_id="load_carts_to_bigquery",
            source_path="cleaned/carts_cleaned.json",
            table_name="carts",
        )

        load_products_to_bigquery = load_to_bigquery(
            task_id="load_products_to_bigquery",
            source_path="cleaned/products_cleaned.json",
            table_name="products",
        )

    @task_group(group_id="run_queries", tooltip="Run queries on BigQuery")
    def run_queries():
        """
        Task group insert jobs to run queries on BigQuery.

        Tasks:
            create_user_summary: Runs the user summary query on BigQuery.
            category_summary: Runs the category summary query on BigQuery.
            cart_details: Runs the cart details query on BigQuery.
        """
        create_user_summary = run_big_query_queries(
            task_id="query_user_summary",
            query=USER_SUMMARY_QUERY
        )

        category_summary = run_big_query_queries(
            task_id="query_category_summary",
            query=CATEGORY_SUMMARY_QUERY
        )

        cart_details = run_big_query_queries(
            task_id="query_cart_details",
            query=CART_DETAILS_QUERY
        )

    extract_tasks = extract()
    aggregated_data = aggregate_data(*extract_tasks)

    (
        extract_tasks
        >> aggregated_data
        >> transform()
        >> load()
        >> create_dummy_dataset
        >> load_data()
        >> run_queries()
    )


pipeline_dag = dummy_pipeline()
