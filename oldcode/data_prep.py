import configparser
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from snowflake.snowpark.functions import sproc
import os
import snowflake.connector
import logging

# Set the logging level to DEBUG
logging.basicConfig(level=logging.DEBUG)

def query_data(conn, query): 

    #@sproc(name="query_data", is_permanent=True, stage_location="@model_stage", replace=True, packages=["snowflake-snowpark-python"], session=session)
    #def query_data(session, query):

    cur = conn.cursor()
    try:
        cur.execute(query)
        result = cur.fetchall()
        return result
    finally:
        cur.close()
q
def run_query_data():
    conn = snowflake.connector.connect(
            account = os.environ["SNOWFLAKE_ACCOUNT"],
            user = os.environ["SNOWFLAKE_USER"],
            #private_key_file_pwd = os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"],
            private_key_file = os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"],
            warehouse = os.environ["SNOWFLAKE_WAREHOUSE"],
            role = os.environ["SNOWFLAKE_ROLE"],
            database = os.environ["SNOWFLAKE_DATABASE"],
            schema = os.environ["SNOWFLAKE_SCHEMA"]
        )
    
    # Define your query
    query_trial = "SELECT * FROM DEV_INGEST.SAMPLE_TRIALS.NCT01714739"
    query_demo = "SELECT * FROM DEV_INGEST.DBT_OMOP.ONCO_DEMOGRAPHICS"
    
    # Run the query
    trial_data = query_data(conn, query_trial)
    demo_data = query_data(conn, query_demo)
    
    success = "Trial Data and Demographic Data Prepped"
    print(success)
    conn.close()  

if __name__ == "__main__":
    run_query_data()