import configparser
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
import os
from snowflake_queries import connect_to_snowflake, get_demo_data, get_age_feature_data, find_patients_age, democount, modelcount
from helpers import execute_sql, create_table, insert_data, create_model_stage

def create_age_monitor_task(session: snowpark.Session, database, schema, warehouse):
    create_age_task_cmd = f"""
        CREATE OR REPLACE TASK {database}.{schema}.AGE_MODEl_MONITOR_TASK
            WAREHOUSE = {warehouse}
            SCHEDULE = 'USING CRON 0 4 * * * UTC'
            AS
                CALL {database}.{schema}.CREATE_AGE_MONITOR()
    """
    df_create_age_task = session.sql(create_age_task_cmd).collect()

def run_task_creation():

    conn = connect_to_snowflake()
    session = Session.builder.configs({
        "connection": conn,
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "private_key_path": os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "role": os.environ["SNOWFLAKE_ROLE"],
        "database": os.environ["SNOWFLAKE_DATABASE"],
        "schema": os.environ["SNOWFLAKE_SCHEMA"]
    }).create()

    data_base = "MITSUI_DEV"
    schema = "MODELS"
    warehouse = "COMPUTE_WH"

    create_age_task(session, data_base, schema, warehouse)

if __name__ == "__main__":
    run_task_creation()