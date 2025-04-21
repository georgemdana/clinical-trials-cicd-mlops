import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from snowflake_queries import connect_to_snowflake, get_trial_data, get_patient_data, get_age_feature_data, find_patients_age
from helpers import execute_sql, create_table, insert_data
import pandas as pd
import re #regex

def process_data():
##################################    TRIAL INFORMATION    ##################################
    # Step 1: Connect to Snowflake
    conn = connect_to_snowflake()

    # Step 2: Fetch trial and demographic dat in a
    trial_data = get_trial_data(conn)  # Assuming it's a DataFrame
    patient_data = get_patient_data(conn)

    # Extract the eligibility criteria column
    incl_excl_col = trial_data["INCLUSION_CRITERIA"]
    study_id = trial_data["TRIAL_ID"]
    df = pd.DataFrame(incl_excl_col)
    df_studyid = pd.DataFrame(study_id)

    # Concatenate df_studyid to the beginning of df
    df = pd.concat([df_studyid, df], axis=1)

    # Regular expression to search for age mentions and preceding characters
    age_pattern = r"([<>≥≤]?)\s*(\d+)\s*(?:years of age|years old|years)?"

    # Extract ages from the text
    def find_ages(text):
        return re.findall(age_pattern, text)

    # Apply the regex search to each row in the 'PROTOCOLSECTION_ELIGIBILITYMODULE_ELIGIBILITYCRITERIA' column
    df['Ages'] = df['INCLUSION_CRITERIA'].apply(find_ages)

    # Now split the 'Ages' column into 'Operator' and 'Age' columns
    df['Operator'] = df['Ages'].apply(lambda x: [op[0] for op in x if op[0]] if x else None)  # Only keep operators that are not empty
    df['Age'] = df['Ages'].apply(lambda x: [int(op[1]) for op in x if op[0]] if x else None)
    df.drop(columns=['Ages'], inplace=True)

    def find_min_max_ages(operators, ages):
        min_age = None
        max_age = None

        if not ages:  # Handle case where ages are empty
            return None, 110  # Default max_age to 110 if no max is found

        for operator, age in zip(operators, ages):
            if age is not None:
                age = int(age)  # Ensure age is an integer
                if operator in ['>', '≥']:
                    # Update min_age if the operator is '>' or '≥'
                    min_age = max(min_age or 0, age)
                elif operator in ['<', '≤']:
                    # Update max_age if the operator is '<' or '≤'
                    max_age = min(max_age or float('inf'), age)

        # If no max_age was set, default it to 110
        if max_age is None:
            max_age = 110

        return min_age, max_age

    # Calculate min and max ages for each row
    df['min_age'], df['max_age'] = zip(*df.apply(lambda row: find_min_max_ages(row['Operator'], row['Age']), axis=1))

    # Set the DataFrame name
    table_name = "FEATURE_AGE"
    data_base = "FEATURES"
    schema = "FEATURE_STORE"
    df.drop(columns='INCLUSION_CRITERIA', inplace = True)

    sql_commands = create_table(df, table_name, data_base, schema)
    for command in sql_commands:
        execute_sql(conn, command)

    # Insert the DataFrame into the Snowflake table
    insert_commands = insert_data(df, table_name, data_base, schema)
    for command in insert_commands:
        execute_sql(conn, command)

##################################    PATIENT INFORMATION    ##################################

    # Step 2: Fetch trial and demographic data
    patient_data = get_patient_data(conn)
    age_feature = get_age_feature_data(conn)
    age_patients = find_patients_age(conn)

    tablename = "FEATURE_AGE_PATIENTS"
    database = "FEATURES"
    schema = "FEATURE_STORE"

    # Create the Snowflake table
    sql_commands = create_table(age_patients, tablename, database, schema)
    for command in sql_commands:
        execute_sql(conn, command)

    # Insert the DataFrame into the Snowflake table
    insert_commands = insert_data(age_patients, tablename, database, schema)
    for command in insert_commands:
        execute_sql(conn, command)

    print("Successfully Loaded Patients for Age Feature")
    
    # Commit the changes
    conn.commit()

    # Close the connection
    conn.close()

if __name__ == "__main__":
    process_data()
