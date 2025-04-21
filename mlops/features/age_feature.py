import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from snowflake_queries import connect_to_snowflake, get_trial_data, get_patient_data, get_age_feature_data, find_patients_age
from helpers import execute_sql, create_table, insert_data
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
import snowflake.connector
from snowflake.snowpark.functions import col
from snowflake.ml.feature_store import CreationMode, Entity, FeatureStore, FeatureView 
import pandas as pd
import re #regex

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
        schema = "FEATURE_STORE"
    )
    return conn

def process_data():
##################################    PATIENT INFORMATION    ##################################

    # Create a Snowpark session
    conn = connect_to_snowflake()
    session = Session.builder.configs({
        "connection": conn,
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "private_key_path": os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "role": os.environ["SNOWFLAKE_ROLE"],
        "database": os.environ["SNOWFLAKE_DATABASE"],
        #"schema": os.environ["SNOWFLAKE_SCHEMA"]
        "schema": "FEATURE_STORE"
    }).create()

    # Define the training data
    query = "SELECT * FROM MITSUI_DEV.FEATURE_STORE.PATIENT_AGE"
    df = session.sql(query).select("PATIENT_ID", "AGE")

    # Set the DataFrame name 
    table_name = "PATIENT_AGE"
    data_base = "MITSUI_DEV"
    schema = "FEATURE_STORE"
    warehouse = "COMPUTE_WH"

    fs = FeatureStore(
        session=session,
        database=data_base,
        name=schema,
        default_warehouse=warehouse,
        creation_mode=CreationMode.CREATE_IF_NOT_EXIST,
    )

    patage_entity = Entity(
        name="PATIENT_ID",
        join_keys=['PATIENT_ID'],
        desc="Patient ID"
    )
    fs.register_entity(patage_entity)
        
    # fs.list_entities().show() #Debug

    # by not using refresh freq this should generate a view not a dynamic table 
    # EX in the name indicates a view or external feature vs dynamic table 
    external_fv = FeatureView(
        name="AGE_FEATURE_VIEW",
        entities=[patage_entity],
        feature_df=df,
        refresh_freq=None,      # None = Feature Store will never refresh the feature data
        desc="AGE Feature View"
    )

    registered_fv: FeatureView = fs.register_feature_view(
        feature_view=external_fv,    
        version= "test",
        block=False,         # whether function call blocks until initial data is available
        overwrite=True,    # whether to replace existing feature view with same name/version
    )    

    print("Successfully registered AGE_FEATURE_VIEW")

if __name__ == "__main__":
    process_data()

    # sql_commands = create_table(df, table_name, data_base, schema)
    # for command in sql_commands:
    #     execute_sql(conn, command)

    # # Insert the DataFrame into the Snowflake table
    # insert_commands = insert_data(df, table_name, data_base, schema)
    # for command in insert_commands:
    #     execute_sql(conn, command)

    # print("Successfully Loaded Patients for Age Feature")