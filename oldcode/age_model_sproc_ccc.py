import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from snowflake.snowpark.functions import sproc
from snowflake_queries import connect_to_snowflake, get_demo_data, get_age_feature_data, find_patients_age, democount, modelcount
from helpers import execute_sql, create_table, insert_data, create_model_stage
import pandas as pd
import re #regex
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
import snowflake.connector
import os

# Use the % matching to predict the likelyhood of a patients match to the clinical trial
def create_age_model(session: snowpark.Session): 

    def create_snowpark_session():
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

        return session

    session = create_snowpark_session()

    @sproc(name="create_age_model", is_permanent=True, stage_location="@model_stage", replace=True, packages=["snowflake-snowpark-python"], session=session)
    def create_age_model(session: Session) -> str:

        # Fetch trial and demographic data
        conn = connect_to_snowflake()
        demo_data = get_demo_data(conn)
        age_feature = get_age_feature_data(conn)
        age_patients = find_patients_age(conn)

        # Create pandas DataFrames from the results
        patient_df = pd.DataFrame(demo_data, columns=['PATIENTID', 'AGE'])
        trial_df = pd.DataFrame(age_feature, columns=['PROTOCOLSECTION_IDENTIFICATIONMODULE_NCTID', 'MIN_AGE', 'MAX_AGE'])

        # Create a new column in the patient DataFrame indicating whether the patient is eligible for each trial
        for index, trial in trial_df.iterrows():
            patient_df[f'trial_{trial.trial_id}_eligible'] = patient_df.apply(
                lambda row: 1 if row.age >= trial.age_min and row.age <= trial.age_max else 0,
                axis=1
            )

        # Define the features and target variable
        features = ['AGE']
        targets = [f'trial_{trial.trial_id}_eligible' for trial in trial_df.itertuples()]

        # Create a new DataFrame with the features and target variable
        data_df = patient_df[features + targets]

        # Loop through each trial and train a model to predict eligibility
        for target in targets:
            # Split the data into training and testing sets
            X = data_df[features]
            y = data_df[target]
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

            # Create a random forest classifier model
            model = RandomForestClassifier()

            # Train the model
            model.fit(X_train, y_train)

            # Make predictions on the testing set
            y_pred = model.predict(X_test)

            # Evaluate the model using accuracy score
            accuracy = accuracy_score(y_test, y_pred)
            print(f"Accuracy for trial {target}: {accuracy}")

            # Make predictions on new data
            new_data = pd.DataFrame({'age': [25]})
            new_prediction = model.predict(new_data)
            print(f"Predicted eligibility for trial {target}: {new_prediction[0]}")

def run_model_creation():
    conn = connect_to_snowflake()

    def create_snowpark_session():
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

        return session

    session = create_snowpark_session()

    database = "DEV_INGEST"
    schema = "MODEL"
    
    # Create the Snowflake table
    sql_commands = create_model_stage(database, schema)
    for command in sql_commands:
        execute_sql(conn, command)
        return 'Stage Created'
    
    create_age_model(session)
    print("Success")

if __name__ == "__main__":
    run_model_creation()