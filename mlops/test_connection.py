import os
import snowflake.connector
import logging

# Set the logging level to DEBUG
logging.basicConfig(level=logging.DEBUG)

def test_connection():
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

    cur = conn.cursor()
    cur.execute('SELECT current_version()')
    print(f'Current Snowflake Version: {cur.fetchone()[0]}')

    cur.close()
    conn.close()

if __name__ == "__main__":
    test_connection()
