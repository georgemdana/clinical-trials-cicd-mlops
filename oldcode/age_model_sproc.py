import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from snowflake_queries import connect_to_snowflake
from helpers import execute_sql, create_table, insert_data, create_model_stage
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from snowflake.snowpark.functions import sproc
import pandas as pd
import re  # regex
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
import snowflake.connector
from snowflake.ml.registry import Registry
import joblib

# Establish Snowflake connection
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

# Enable custom package usage
session.custom_package_usage_config['enabled'] = True
session.add_packages(
    "scikit-learn",
    "pandas",
    "snowflake-ml-python",
    "joblib",
    "cloudpickle"
)

# Create the Snowflake table
data_base = "MITSUI_DEV"
schema_model = "MODELS"
sql_commands = create_model_stage(data_base, schema_model)
for command in sql_commands:
    execute_sql(conn, command)

def create_age_model_top(session: snowpark.Session): 

    @sproc(name="create_age_model", is_permanent=True, stage_location="@model_stage", replace=True, packages=["snowflake-snowpark-python", "joblib", "cloudpickle", "scikit-learn", "pandas", "snowflake-ml-python"])
    def create_age_model(session: Session) -> str:

        def query_data(session, query):
            """
            Executes a SQL query on the Snowflake connection and returns the results.
            """
            result = session.sql(query)
            return result.to_pandas()

        patient_age_query = "SELECT * FROM MITSUI_DEV.FEATURE_STORE.PATIENT_AGE"
        patient_age = query_data(session, patient_age_query)
        patient_age.columns = ['PATIENT_ID', 'AGE']

        trial_age_query = "SELECT * FROM MITSUI_DEV.FEATURE_STORE.TRIAL_AGE"
        trial_age = query_data(session, trial_age_query)
        trial_age.columns = ['TRIAL_ID', 'INCLUSION_CRITERIA', 'AGES', 'MIN_AGE', 'MAX_AGE']

        # Create all possible patient-trial combinations
        combinations = pd.merge(patient_age, trial_age, how="cross")

        # Create target variable
        combinations['is_eligible'] = (combinations['AGE'] >= combinations['MIN_AGE']) & (combinations['AGE'] <= combinations['MAX_AGE'])

        # Feature engineering - add an age difference from ranges
        combinations['age_diff_from_min'] = combinations['AGE'] - combinations['MIN_AGE']
        combinations['age_diff_from_max'] = combinations['MAX_AGE'] - combinations['AGE']

        # Prepare features and target
        X = combinations[['AGE', 'MIN_AGE', 'MAX_AGE', 'age_diff_from_min', 'age_diff_from_max']]
        y = combinations['is_eligible']

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Train RandomForestClassifier
        rf_model = RandomForestClassifier(n_estimators=100, random_state=42)
        rf_model.fit(X_train, y_train)

        # Predict probabilities
        y_pred_proba = rf_model.predict_proba(X_test)[:, 1]  # Probability of class 1 (eligible)

        # Add predictions to test data
        test_results = X_test.copy()
        test_results['PATIENT_ID'] = combinations.loc[X_test.index, 'PATIENT_ID']
        test_results['TRIAL_ID'] = combinations.loc[X_test.index, 'TRIAL_ID']
        test_results['match_percentage'] = y_pred_proba * 100

        # Display results
        print(test_results[['PATIENT_ID', 'TRIAL_ID', 'match_percentage']].head(10))

        # Evaluate model performance
        y_pred = rf_model.predict(X_test)
        print("\nModel Accuracy:", accuracy_score(y_test, y_pred))
        print("\nClassification Report:")
        print(classification_report(y_test, y_pred))

        # Register the model
        model_name = "rfp_model_patient_matching_age"
        model_version = "one"

        # Create the registry object
        registry = Registry(session=session, database_name="MITSUI_DEV", schema_name="MODELS")

        # Serialize the model using joblib
        joblib.dump(rf_model, 'rf_model.joblib')

        # Load the serialized model into the Registry
        s_model = joblib.load('rf_model.joblib')
        registry.log_model(s_model,
                model_name = model_name,
                version_name = model_version,
                conda_dependencies=[
                    "scikit-learn",
                    "pandas",
                    "snowflake-ml-python",
                    "joblib",
                    "cloudpickle"
                ],
                comment="Mitsui test age model",
                sample_input_data=X_train)
        
        # Create table for testing data
        table_name = "DEMO_AGE_MODEL_TESTDATA"
        data_base = "MITSUI_DEV"
        schema = "MODELS"

        sql_commands = create_table(X, table_name, data_base, schema)
        for command in sql_commands:
            execute_sql(conn, command)

        # Insert the DataFrame into the Snowflake table
        insert_commands = insert_data(X, table_name, data_base, schema)
        for command in insert_commands:
            execute_sql(conn, command)

        print(f"Model {model_name} version {model_version} monitor built successfully.")
     
def run_model_creation():
    create_age_model_top(session)
    print("Stored procedure created successfully.")

if __name__ == "__main__":
    run_model_creation()
