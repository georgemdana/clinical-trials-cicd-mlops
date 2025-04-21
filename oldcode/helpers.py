import pandas as pd

# Function to run sql
def execute_sql(conn, command):
    cur = conn.cursor()
    try:
        cur.execute(command)
        return cur.fetchall()
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    finally:
        cur.close()

def map_dtype_to_snowflake(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_string_dtype(dtype):
        return "VARCHAR(16777216)"  # or any size you need
    elif pd.api.types.is_object_dtype(dtype):
        return "VARIANT"  # for lists or mixed types
    else:
        return "VARCHAR(16777216)"  # default case

def create_table(df, table_name, database_name, schema_name):
    usedb = f"USE DATABASE {database_name};"
    useschema = f"USE SCHEMA {schema_name};"
    userole = f"USE ROLE accountadmin;"
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"

    # Loop through DataFrame columns to define their types
    for col_name, dtype in zip(df.columns, df.dtypes):
        data_type = map_dtype_to_snowflake(dtype)
        create_table_query += f"{col_name} {data_type},\n"

    create_table_query = create_table_query.rstrip(',\n') + "\n);"
    return [usedb, useschema, userole, create_table_query]


def insert_data(df, table_name, database_name, schema_name):
    usedb = f"USE DATABASE {database_name};"
    useschema = f"USE SCHEMA {schema_name};"
    userole = f"USE ROLE accountadmin;"

    insert_statement = f"INSERT INTO {schema_name}.{table_name} ({', '.join(df.columns)}) VALUES "
    
    batch_size = 1000
    sql_commands = [usedb, useschema, userole]

    max_length = 16777216  # Adjust this length as necessary

    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i + batch_size]
        value_strings = []
        for index in range(len(batch)):
            row_values = []
            for col_name, value in zip(df.columns, batch.iloc[index].values):
                if pd.isna(value):
                    row_values.append('NULL')
                elif isinstance(value, list):
                    # Convert list to JSON for VARIANT column
                    json_value = f"'{pd.Series(value).to_json()}'"
                    row_values.append(json_value)
                elif isinstance(value, str):
                    # Truncate long strings to avoid errors
                    if len(value) > max_length:
                        value = value[:max_length]
                    row_values.append(f"'{value}'")
                else:
                    row_values.append(str(value))
            value_strings.append(f"({', '.join(row_values)})")
        
        batch_insert = insert_statement + ",\n".join(value_strings) + ";"
        sql_commands.append(batch_insert)

    return sql_commands

def create_model_stage(database_name, schema_name):
    usedb = f"USE DATABASE {database_name};"
    useschema = f"USE SCHEMA {schema_name};"
    userole = f"USE ROLE accountadmin;"
    create_stage_query = f"CREATE STAGE IF NOT EXISTS {database_name}.{schema_name}.model_stage;" # (\n"
    #create_stage_query = create_stage_query.rstrip(',\n') + "\n);"
    return [usedb, useschema, userole, create_stage_query]
