from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

@dag(dag_id='BuildSummary', default_args=default_args, schedule_interval=None, catchup=False)
def etl_dag():
    
    @task
    def run_ctas():
        # Step 1: Create table
        logging.info("Creating table dev.analytics.session_summary...")
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                CREATE OR REPLACE TABLE dev.analytics.session_summary AS 
                SELECT u.*, s.ts
                FROM dev.raw_data.user_session_channel u
                JOIN dev.raw_data.session_timestamp s ON u."SESSIONID" = s."SESSIONID"
            """)
            logging.info("Table created successfully.")

            # Step 2: Print number of rows loaded into the table
            cursor.execute("SELECT COUNT(1) FROM dev.analytics.session_summary")
            rows_loaded = cursor.fetchone()
            if rows_loaded:
                logging.info(f"Number of rows loaded into session_summary: {rows_loaded[0]}")
            else:
                logging.info("No rows loaded into session_summary.")

            # Step 3: Check for duplicates
            logging.info("Checking for duplicates using query: SELECT SESSIONID, COUNT(1) AS cnt FROM dev.analytics.session_summary GROUP BY 1 HAVING COUNT(1) > 1")
            cursor.execute("""
                SELECT SESSIONID, COUNT(1) AS cnt 
                FROM dev.analytics.session_summary 
                GROUP BY 1 
                HAVING COUNT(1) > 1
            """)
            result = cursor.fetchone()
            
            if result and len(result) > 1 and int(result[1]) > 1:
                logging.error("Duplicates found in session_summary.")
                raise ValueError("Duplicate records found.")
            else:
                logging.info("No duplicates found.")

            # Step 4: Check primary key uniqueness
            logging.info("Checking primary key uniqueness using query: SELECT SESSIONID, COUNT(1) AS cnt FROM dev.analytics.session_summary GROUP BY 1 ORDER BY 2 DESC LIMIT 1")
            cursor.execute("""
                SELECT SESSIONID, COUNT(1) AS cnt 
                FROM dev.analytics.session_summary 
                GROUP BY 1 
                ORDER BY 2 DESC 
                LIMIT 1
            """)
            result = cursor.fetchone()

            if result and len(result) > 1 and int(result[1]) > 1:
                logging.error("Primary key violation detected.")
                raise ValueError("Primary key uniqueness violated.")
            else:
                logging.info("Primary key uniqueness verified.")

            logging.info("CTAS operation and validation checks completed successfully.")

        except Exception as e:
            logging.error(f"Failed to execute SQL. Rolled back changes! Error: {str(e)}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    run_ctas()

etl_dag = etl_dag()
