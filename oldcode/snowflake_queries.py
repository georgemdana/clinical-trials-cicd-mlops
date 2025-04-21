import os
import snowflake.connector
import pandas as pd

def query_data(conn, query):
    """
    Executes a SQL query on the Snowflake connection and returns the results.
    """
    cur = conn.cursor()
    try:
        cur.execute(query)
        # Fetch all rows and column names
        data = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        # Create a DataFrame directly
        return pd.DataFrame(data, columns=columns)  # Return a DataFrame
    finally:
        cur.close()

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
        schema = os.environ["SNOWFLAKE_SCHEMA"]
    )
    return conn


def get_age_feature_data(conn):
    """
    Fetches trial data from Snowflake.
    """
    query_age_feature = "SELECT * FROM MITSUI_DEV.FEATURE_STORE.FEATURE_AGE"
    return query_data(conn, query_age_feature)

def get_demo_data(conn):
    """
    Fetches demographic data from Snowflake.
    """
    query_demo = "SELECT * FROM MITSUI_DEV.DATASCIENCE_DEV.PATIENT_DATA"
    return query_data(conn, query_demo)

def find_patients_age(conn):
    query_find_patients_age = "SELECT DISTINCT D.PATIENT_ID, D.AGE, F.MIN_AGE, F.MAX_AGE FROM MITSUI_DEV.DATASCIENCE_DEV.PATIENT_DATA D JOIN MITSUI_DEV.FEATURE_STORE.FEATURE_AGE F ON D.AGE BETWEEN F.MIN_AGE AND F.MAX_AGE"
    return query_data(conn, query_find_patients_age)

def democount(conn):
    """
    Fetches accuracy
    """
    query_demo = "SELECT DISTINCT * FROM MITSUI_DEV.DATASCIENCE_DEV.PATIENT_DATA WHERE AGE >= 18"
    return query_data(conn, query_demo)

def modelcount(conn):
    """
    Fetches accuracy
    """
    query_demo = "SELECT DISTINCT * FROM MITSUI_DEV.MODELS.FEATURE_AGE_PATIENTS"
    return query_data(conn, query_demo)