import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from snowflake_queries import connect_to_snowflake, get_trial_data, get_patient_data, get_age_feature_data, find_patients_age
from helpers import execute_sql, create_table, insert_data
import pandas as pd
import re #regex

def process_data():
##################################    PATIENT INFORMATION    ##################################
    # Step 1: Connect to Snowflake
    conn = connect_to_snowflake()
    trial_data = get_trial_data(conn)
    trial_data = pd.DataFrame(trial_data)
    df = trial_data[["TRIAL_ID", "INCLUSION_CRITERIA"]]

    # Regular expression to search for age mentions with range or comparison operators
    age_pattern = r"(?:between\s*(\d+)\s*and\s*(\d+)|aged\s*(\d+)\s*to\s*(\d+)|(?:over|more than)\s*(\d+)|(?:under|less than)\s*(\d+)|([<>≥≤]?)\s*(\d+)\s*(?:years of age|years old|years)?)"

    # Extract ages from the text
    def find_ages(text):
        matches = re.findall(age_pattern, text, re.IGNORECASE)
        results = []
        for match in matches:
            if match[0] and match[1]:  # Case for "between X and Y"
                results.append(('between', int(match[0]), int(match[1])))
            elif match[2] and match[3]:  # Case for "aged X to Y"
                results.append(('between', int(match[2]), int(match[3])))
            elif match[4]:  # Case for "over X" or "more than X"
                results.append(('>', int(match[4])))
            elif match[5]:  # Case for "under X" or "less than X"
                results.append(('<', int(match[5])))
            else:  # Case for comparison operators like >, <, ≥, ≤
                operator = match[6] if match[6] else '='  # Default to '=' if no operator
                results.append((operator, int(match[7])))
        return results

    # Apply the regex search to each row in the 'PROTOCOLSECTION_ELIGIBILITYMODULE_ELIGIBILITYCRITERIA' column
    df = df.assign(Ages=df['INCLUSION_CRITERIA'].apply(find_ages))

    # Function to calculate min and max ages based on the extracted operators and ages
    def find_min_max_ages(ages):
        min_age = None
        max_age = None

        if not ages:  # Handle case where ages are empty
            return None, 110  # Default max_age to 110 if no max is found

        for age_info in ages:
            if age_info[0] == 'between':
                # For "between X and Y", directly set min and max
                min_age = age_info[1]
                max_age = age_info[2]
            else:
                operator, age = age_info[0], age_info[1]
                if operator in ['>', '≥']:
                    min_age = max(min_age or 0, age)
                elif operator in ['<', '≤']:
                    max_age = min(max_age or float('inf'), age)
                elif operator == '=' and (min_age is None or max_age is None):
                    # Handle cases with equal age, set both min and max
                    min_age = max_age = age

        # If no max_age was set, default it to 110
        if max_age is None:
            max_age = 110

        return min_age, max_age

    # Calculate min and max ages for each row
    df['min_age'], df['max_age'] = zip(*df['Ages'].apply(find_min_max_ages))
    # Assuming df is your DataFrame
    pd.set_option('display.max_rows', None)        # Display all rows
    pd.set_option('display.max_columns', None)     # Display all columns
    pd.set_option('display.max_colwidth', None)    # Display full column width, prevents truncating
    pd.set_option('display.width', None)           # Automatically adjust display width for the DataFrame

    print(df)

    # Set the DataFrame name
    table_name = "TRIAL_AGE"
    data_base = "MITSUI_DEV"
    schema = "FEATURE_STORE"

    sql_commands = create_table(df, table_name, data_base, schema)
    for command in sql_commands:
        execute_sql(conn, command)

    # Insert the DataFrame into the Snowflake table
    insert_commands = insert_data(df, table_name, data_base, schema)
    for command in insert_commands:
        execute_sql(conn, command)

    print("Successfully Loaded Clinical Trial Information for Age Feature")
    
    # Commit the changes
    conn.commit()

    # Close the connection
    conn.close()

if __name__ == "__main__":
    process_data()
