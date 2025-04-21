import configparser
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
import snowflake.connector
import os

def create_age_stream(session: snowpark.Session, database, schema, warehouse):
    create_age_stream_cmd = f"""
    CREATE OR REPLACE STREAM AGE_MODEL_STREAM
    ON TABLE MITSUI_DEV.MODELS.X_TEST
    APPEND_ONLY = FALSE;  -- Use FALSE to track all types of changes (INSERT, UPDATE, DELETE)
    """
    df_create_age_stream = session.sql(create_age_stream_cmd).collect()

def create_age_task(session: snowpark.Session, database, schema, warehouse):
    create_age_task_cmd = f"""
        CREATE OR REPLACE TASK MITSUI_DEV.MODELS.AGE_MODEL_TASK
            WAREHOUSE = {warehouse}
            WHEN 
                SYSTEM$STREAM_HAS_DATA('{database}.{schema}.AGE_MODEL_STREAM')  -- Check if the stream has data
            AS
                CALL MITSUI_DEV.MODELS.TRAIN_MODEL()
    """
    df_create_age_task = session.sql(create_age_task_cmd).collect()

def run_task_creation():

    def connect_to_snowflake():
        """
        Establishes a connection to Snowflake using environment variables.
        Returns the connection object.
        """
        conn = snowflake.connector.connect(
            account = os.environ["SNOWFLAKE_ACCOUNT"],
            user = os.environ["SNOWFLAKE_USER"],
            private_key_file = os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"],
            warehouse = os.environ["SNOWFLAKE_WAREHOUSE"],
            role = os.environ["SNOWFLAKE_ROLE"],
            database = os.environ["SNOWFLAKE_DATABASE"],
            schema = "MODELS"
        )
        return conn
    
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
    schema_ = "MODELS"
    warehouse = "COMPUTE_WH"

    create_age_stream(session, data_base, schema_, warehouse)
    create_age_task(session, data_base, schema_, warehouse)

if __name__ == "__main__":
    run_task_creation()