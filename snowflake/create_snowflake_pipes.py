import snowflake.connector
import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

print("Connecting to Postgres database...")
postgres_conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    dbname=os.getenv("POSTGRES_DB"),
)

postgres_cursor = postgres_conn.cursor()
print("Connected to Postgres successfully!\n")


print("Connecting to Snowflake database...")
snowflake_conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DB"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

snowflake_cursor = snowflake_conn.cursor()
print("Connected to Snowflake successfully!\n")

# Get all the table names
postgres_cursor.execute(
    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
)
table_names = postgres_cursor.fetchall()
tables = [name[0] for name in table_names]

postgres_cursor.close()
postgres_conn.close()

for table in tables:
    create_pipe_sql = f"""
                    CREATE OR REPLACE PIPE {table}_pipe
                        AUTO_INGEST = TRUE
                        INTEGRATION = ECOMMERCE_GCS_NOTIFY_INT
                        AS
                        COPY INTO ECOMMERCE.RAW.{table} (v)
                        FROM @ECOMMERCE.RAW.ECOMMERCE_GCS_STAGE/{table}/
                        """
    try:
        snowflake_cursor.execute(create_pipe_sql)
        print(f"Successfully created {table} pipe")
    except Exception as e:
        print(f"Error creating pipe for {table}: {e}")

snowflake_cursor.close()
snowflake_conn.close()
print("\nPipeline setup complete!")
