from datetime import datetime
from airflow.sdk import dag
from airflow.datasets import Dataset
from airflow.providers.standard.operators.bash import BashOperator

staging_files_ready = Dataset("ecommerce://staged_files_ready")


@dag(
    schedule=staging_files_ready,
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["snowflake_marts"],
)
def snowflake_marts():

    dim_customers = BashOperator(
        task_id="dim_customers_table",
        bash_command="cd /opt/airflow/ecommerce_dbt && dbt run -s dim_customers --profiles-dir /home/airflow/.dbt",
    )

    dim_dates = BashOperator(
        task_id="dim_dates_table",
        bash_command="cd /opt/airflow/ecommerce_dbt && dbt run -s dim_dates --profiles-dir /home/airflow/.dbt",
    )

    dim_orders = BashOperator(
        task_id="dim_orders_table",
        bash_command="cd /opt/airflow/ecommerce_dbt && dbt run -s dim_orders --profiles-dir /home/airflow/.dbt",
    )

    dim_products = BashOperator(
        task_id="dim_products_table",
        bash_command="cd /opt/airflow/ecommerce_dbt && dbt run -s dim_products --profiles-dir /home/airflow/.dbt",
    )

    dim_sellers = BashOperator(
        task_id="dim_sellers_table",
        bash_command="cd /opt/airflow/ecommerce_dbt && dbt run -s dim_sellers --profiles-dir /home/airflow/.dbt",
    )

    fact_sales = BashOperator(
        task_id="fact_sales_table",
        bash_command="cd /opt/airflow/ecommerce_dbt && dbt run -s fact_sales --profiles-dir /home/airflow/.dbt",
    )

    fact_payments = BashOperator(
        task_id="fact_payments_table",
        bash_command="cd /opt/airflow/ecommerce_dbt && dbt run -s fact_payments --profiles-dir /home/airflow/.dbt",
    )


snowflake_marts()
