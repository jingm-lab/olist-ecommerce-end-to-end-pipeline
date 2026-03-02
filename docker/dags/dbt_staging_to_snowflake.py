from datetime import datetime
from airflow.sdk import dag
from airflow.datasets import Dataset
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

staging_files_ready = Dataset("ecommerce://staged_files_ready")


@dag(
    schedule="@daily",
    start_date=datetime(2026, 2, 20),
    catchup=False,
    tags=["dbt_staging"],
)
def stage_to_snowflake():

    stage_customers = BashOperator(
        task_id="stage_customers_table",
        bash_command="cd /opt/airflow/ecommerce_dbt && dbt run -s stg_customers --profiles-dir /home/airflow/.dbt",
    )

    stage_geolocations = BashOperator(
        task_id="stage_geolocations_table",
        bash_command="cd /opt/airflow/ecommerce_dbt && dbt run -s stg_geolocations --profiles-dir /home/airflow/.dbt",
    )

    stage_order_items = BashOperator(
        task_id="stage_order_items_table",
        bash_command="cd /opt/airflow/ecommerce_dbt && dbt run -s stg_order_items --profiles-dir /home/airflow/.dbt",
    )

    stage_order_payments = BashOperator(
        task_id="stage_order_payments_table",
        bash_command="cd /opt/airflow/ecommerce_dbt && dbt run -s stg_order_payments --profiles-dir /home/airflow/.dbt",
    )

    stage_order_reviews = BashOperator(
        task_id="stage_order_reviews_table",
        bash_command="cd /opt/airflow/ecommerce_dbt && dbt run -s stg_order_reviews --profiles-dir /home/airflow/.dbt",
    )

    stage_orders = BashOperator(
        task_id="stage_orders_table",
        bash_command="cd /opt/airflow/ecommerce_dbt && dbt run -s stg_orders --profiles-dir /home/airflow/.dbt",
    )

    stage_product_category_name_translation = BashOperator(
        task_id="stage_product_category_name_translation_table",
        bash_command="cd /opt/airflow/ecommerce_dbt && dbt run -s stg_product_category_name_translation --profiles-dir /home/airflow/.dbt",
    )

    stage_products = BashOperator(
        task_id="stage_products_table",
        bash_command="cd /opt/airflow/ecommerce_dbt && dbt run -s stg_products --profiles-dir /home/airflow/.dbt",
    )

    stage_sellers = BashOperator(
        task_id="stage_sellers_table",
        bash_command="cd /opt/airflow/ecommerce_dbt && dbt run -s stg_sellers --profiles-dir /home/airflow/.dbt",
    )

    staging_complete = EmptyOperator(
        task_id="staging_complete", outlets=[staging_files_ready]
    )

    [
        stage_customers,
        stage_geolocations,
        stage_order_items,
        stage_order_payments,
        stage_order_reviews,
        stage_orders,
        stage_product_category_name_translation,
        stage_products,
        stage_sellers,
    ] >> staging_complete


stage_to_snowflake()
