# Dockerfile-airflow
FROM apache/airflow:3.1.6

# Switch to airflow user first
USER airflow

# Install dbt packages
RUN pip install --no-cache-dir dbt-core dbt-snowflake