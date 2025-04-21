import pandas as pd
import numpy as np
import json

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
    
def create_model_stage(database_name, schema_name):
    usedb = f"USE DATABASE {database_name};"
    useschema = f"USE SCHEMA {schema_name};"
    userole = f"USE ROLE accountadmin;"
    create_stage_query = f"CREATE STAGE IF NOT EXISTS {database_name}.{schema_name}.model_stage;" # (\n"
    #create_stage_query = create_stage_query.rstrip(',\n') + "\n);"
    return [usedb, useschema, userole, create_stage_query]

# def create_table(df, table_name, database_name, schema_name):
#     # Use f-strings to create SQL commands
#     usedb = f"USE DATABASE {database_name};"
#     useschema = f"USE SCHEMA {schema_name};"
#     userole = f"USE ROLE accountadmin;"
#     create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"

#     # Loop through DataFrame columns to define their types
#     for col_name, dtype in zip(df.columns, df.dtypes):
#         if dtype == 'int64':
#             data_type = "INT"
#         elif dtype == 'float64':
#             data_type = "FLOAT"
#         else:
#             data_type = "VARCHAR(255)"
#         create_table_query += f"{col_name} {data_type},\n"

#     # Remove the last comma and add closing parenthesis
#     create_table_query = create_table_query.rstrip(',\n') + "\n);"

#     # Return a list of SQL commands
#     return [usedb, useschema, userole, create_table_query]

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

    def is_nan(value):
        if isinstance(value, (list, tuple)):
            return len(value) == 0
        if isinstance(value, (pd.Series, np.ndarray)):
            return pd.isna(value).any()
        return pd.isna(value)

    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i + batch_size]
        value_strings = []
        for _, row in batch.iterrows():
            row_values = []
            for col_name, value in row.items():
                if is_nan(value):
                    row_values.append('NULL')
                elif isinstance(value, (list, tuple, pd.Series, np.ndarray)):
                    # Convert list, tuple, or array-like to JSON for VARIANT column
                    json_value = f"'{json.dumps(value.tolist() if isinstance(value, (pd.Series, np.ndarray)) else value)}'"
                    row_values.append(json_value)
                elif isinstance(value, str):
                    # Truncate long strings to avoid errors
                    if len(value) > max_length:
                        value = value[:max_length]
                    # Escape single quotes in the string
                    value = value.replace("'", "''")
                    row_values.append(f"'{value}'")
                else:
                    # Handle non-string, non-list values (e.g., numbers)
                    row_values.append(str(value))
            value_strings.append(f"({', '.join(row_values)})")
        
        batch_insert = insert_statement + ",\n".join(value_strings) + ";"
        sql_commands.append(batch_insert)

    return sql_commands

# def insert_data(df, table_name, database_name, schema_name):
#     usedb = f"USE DATABASE {database_name};"
#     useschema = f"USE SCHEMA {schema_name};"
#     userole = f"USE ROLE accountadmin;"

#     insert_statement = f"INSERT INTO {schema_name}.{table_name} ({', '.join(df.columns)}) VALUES "
    
#     batch_size = 1000
#     sql_commands = [usedb, useschema, userole]

#     max_length = 16777216  # Adjust this length as necessary

#     for i in range(0, len(df), batch_size):
#         batch = df.iloc[i:i + batch_size]
#         value_strings = []
#         for _, row in batch.iterrows():
#             row_values = []
#             for col_name, value in row.items():
#                 if pd.isna(value).any() if isinstance(value, (pd.Series, np.ndarray)) else pd.isna(value):
#                     row_values.append('NULL')
#                 elif isinstance(value, (list, pd.Series, np.ndarray)):
#                     # Convert list or array-like to JSON for VARIANT column
#                     json_value = f"'{json.dumps(value.tolist() if isinstance(value, (pd.Series, np.ndarray)) else value)}'"
#                     row_values.append(json_value)
#                 elif isinstance(value, str):
#                     # Truncate long strings to avoid errors
#                     if len(value) > max_length:
#                         value = value[:max_length]
#                     # Escape single quotes in the string
#                     value = value.replace("'", "''")
#                     row_values.append(f"'{value}'")
#                 else:
#                     # Handle non-string, non-list values (e.g., numbers)
#                     row_values.append(str(value))
#             value_strings.append(f"({', '.join(row_values)})")
        
#         batch_insert = insert_statement + ",\n".join(value_strings) + ";"
#         sql_commands.append(batch_insert)

#     return sql_commands

# Define a function to insert data into a Snowflake table based on a Pandas DataFrame
# def insert_data(df, table_name, database_name, schema_name):
#     # Use f-strings to create SQL commands
#     usedb = f"USE DATABASE {database_name};"
#     useschema = f"USE SCHEMA {schema_name};"
#     userole = f"USE ROLE accountadmin;"

#     # Create the insert statement
#     insert_statement = f"INSERT INTO {schema_name}.{table_name} ({', '.join(df.columns)}) VALUES "
    
#     # Process rows in batches to avoid extremely long SQL statements
#     batch_size = 1000  # Adjust this value based on your needs
#     sql_commands = [usedb, useschema, userole]

#     for i in range(0, len(df), batch_size):
#         batch = df.iloc[i:i+batch_size]
#         value_strings = []
#         for index in range(len(batch)):
#             row_values = []
#             for value in batch.iloc[index].values:
#                 if pd.isna(value):
#                     row_values.append('NULL')
#                 elif isinstance(value, str):
#                     row_values.append(f"'{value}'")
#                 else:
#                     row_values.append(str(value))
#             value_strings.append(f"({', '.join(row_values)})")
        
#         batch_insert = insert_statement + ",\n".join(value_strings) + ";"
#         sql_commands.append(batch_insert)

#     return sql_commands

# def insert_data(df, table_name, database_name, schema_name):
#     usedb = f"USE DATABASE {database_name};"
#     useschema = f"USE SCHEMA {schema_name};"
#     userole = f"USE ROLE accountadmin;"

#     insert_statement = f"INSERT INTO {schema_name}.{table_name} ({', '.join(df.columns)}) VALUES "
    
#     batch_size = 1000
#     sql_commands = [usedb, useschema, userole]

#     max_length = 16777216  # Adjust this length as necessary

#     for i in range(0, len(df), batch_size):
#         batch = df.iloc[i:i + batch_size]
#         value_strings = []
#         for index in range(len(batch)):
#             row_values = []
#             for col_name, value in zip(df.columns, batch.iloc[index].values):
#                 if pd.isna(value):
#                     row_values.append('NULL')
#                 elif isinstance(value, list):
#                     # Convert list to JSON for VARIANT column
#                     json_value = f"'{pd.Series(value).to_json()}'"
#                     row_values.append(json_value)
#                 elif isinstance(value, str):
#                     # Truncate long strings to avoid errors
#                     if len(value) > max_length:
#                         value = value[:max_length]
#                     row_values.append(f"'{value}'")
#                 else:
#                     row_values.append(str(value))
#             value_strings.append(f"({', '.join(row_values)})")
        
#         batch_insert = insert_statement + ",\n".join(value_strings) + ";"
#         sql_commands.append(batch_insert)

#     return sql_commands

# def insert_data(df, table_name, database_name, schema_name):
#     usedb = f"USE DATABASE {database_name};"
#     useschema = f"USE SCHEMA {schema_name};"
#     userole = f"USE ROLE accountadmin;"

#     insert_statement = f"INSERT INTO {schema_name}.{table_name} ({', '.join(df.columns)}) VALUES "
    
#     batch_size = 1000
#     sql_commands = [usedb, useschema, userole]

#     max_length = 16777216  # Adjust this length as necessary

#     for i in range(0, len(df), batch_size):
#         batch = df.iloc[i:i + batch_size]
#         value_strings = []
#         for index in range(len(batch)):
#             row_values = []
#             for col_name, value in zip(df.columns, batch.iloc[index].values):
#                 if pd.isna(value):
#                     row_values.append('NULL')
#                 elif isinstance(value, (list, pd.Series)):  # Handle lists or Series (array-like values)
#                     # Convert list to JSON for VARIANT column
#                     json_value = f"'{pd.Series(value).to_json()}'"
#                     row_values.append(json_value)
#                 elif isinstance(value, str):
#                     # Truncate long strings to avoid errors
#                     if len(value) > max_length:
#                         value = value[:max_length]
#                     row_values.append(f"'{value}'")
#                 else:
#                     # Handle non-string, non-list values (e.g., numbers)
#                     row_values.append(str(value))
#             value_strings.append(f"({', '.join(row_values)})")
        
#         batch_insert = insert_statement + ",\n".join(value_strings) + ";"
#         sql_commands.append(batch_insert)

#     return sql_commands

