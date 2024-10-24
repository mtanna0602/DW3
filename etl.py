from airflow.decorators import task
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import logging

def return_snowflake_conn():
    try:
        # Initialize SnowflakeHook
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        return hook.get_conn().cursor()
    except Exception as e:
        logging.error(f"Error connecting to Snowflake: {e}")
        raise

@task
def import_table(table_name):
    cur = return_snowflake_conn()
    try:
        # SQL to select all data from the given table
        sql = f"SELECT * FROM dev.raw_data.{table_name};"
        logging.info(f"Importing {table_name}...")
        cur.execute(sql)
        results = cur.fetchall()
        logging.info(f"Fetched {len(results)} rows from {table_name}")
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        raise
    finally:
        # Always close the cursor after using it
        cur.close()

# Define the DAG
with DAG(
    dag_id='Import_User_Session_Data',
    start_date=datetime(2024, 10, 2),
    catchup=False,
    schedule_interval='@daily',  # Set the interval or schedule as needed
    tags=['ETL'],
) as dag:
    # Importing the two specified tables
    user_session_channel_task = import_table('user_session_channel')
    session_timestamp_task = import_table('session_timestamp')

    # Task dependencies if needed
    user_session_channel_task >> session_timestamp_task
