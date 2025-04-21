import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
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
from sklearn.metrics import confusion_matrix

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

def build_model():
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
        "schema": "MODELS"
    }).create()

    session.add_packages(
    "scikit-learn",
    "pandas",
    "snowflake-ml-python")

    # Define a function to train the model
    @sproc(name='train_model',
        is_permanent=True,
        stage_location="@MITSUI_DEV.MODELS.MODEL_STAGE",
        replace=True,
        packages=["snowflake-snowpark-python", "scikit-learn", "pandas", "joblib", "snowflake-ml-python"],
        session=session)
    def train_model(session: Session) -> str:
        # Define the training data
        query = "SELECT * FROM MITSUI_DEV.FEATURE_STORE.PATIENT_AGE"
        patient_age = session.sql(query).collect()

        # Convert the data to a Pandas DataFrame
        df_age = pd.DataFrame(patient_age)

        # Define the training data
        query2 = "SELECT * FROM MITSUI_DEV.FEATURE_STORE.TRIAL_AGE"
        trial_age = session.sql(query2).collect()

        # Convert the data to a Pandas DataFrame
        df_trial = pd.DataFrame(trial_age)

        # Create all possible patient-trial combinations
        combinations = pd.merge(df_age, df_trial, how="cross")

        # Create target variable
        combinations['is_eligible'] = (combinations['AGE'] >= combinations['MIN_AGE']) & (combinations['AGE'] <= combinations['MAX_AGE'])

        # Feature engineering - add an age difference from ranges
        combinations['age_diff_from_min'] = combinations['AGE'] - combinations['MIN_AGE']
        combinations['age_diff_from_max'] = combinations['MAX_AGE'] - combinations['AGE']

        # Prepare features and target
        X = combinations[['AGE', 'MIN_AGE', 'MAX_AGE', 'age_diff_from_min', 'age_diff_from_max']]
        y = combinations['is_eligible']
        combinations['predictions'] = None # placeholder

        # Split the data into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Train a RandomForestClassifier model on the training data
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)

        # Save the model to the model registry
        registry = Registry(session, database_name="MITSUI_DEV", schema_name="MODELS")
        registry.log_model(model,
                        model_name='random_forest',
                        comment='Random forest model trained on dataset',
                        conda_dependencies=['scikit-learn', 'pandas', 'joblib'],
                        sample_input_data=X_train)

        # Predict using the loaded model
        predictions = model.predict(X_test)

        # Update the combinations DataFrame with predictions
        combinations.loc[X_test.index, 'prediction'] = predictions
        
        # Create a DataFrame for output that includes patient ID, trial ID, all X values, and predictions
        output_df = combinations.loc[X_test.index, ['PATIENT_ID', 'TRIAL_ID', 'AGE', 'MIN_AGE', 'MAX_AGE', 
                                                    'age_diff_from_min', 'age_diff_from_max', 
                                                    'prediction']]
        prediction_df = pd.DataFrame(output_df)
        # Convert to Snowpark DataFrame and save
        snowpark_df = session.create_dataframe(prediction_df)
        snowpark_df.write.mode("overwrite").save_as_table("MODEL_OUTPUT", table_type="transient")

        # Calculate accuracy
        accuracy = accuracy_score(y_test, predictions)
        # Generate a classification report
        report = classification_report(y_test, predictions, output_dict=True)
        # Create a confusion matrix
        cm = confusion_matrix(y_test, predictions)

        # Create a DataFrame for accuracy
        accuracy_df = pd.DataFrame({"metric": ["accuracy"], "value": [accuracy]})

        # Create a DataFrame for the classification report
        report_df = pd.DataFrame(report).transpose().reset_index()
        report_df.rename(columns={'index': 'class'}, inplace=True)  # Rename index column to 'class'

        # Create a DataFrame for the confusion matrix
        cm_df = pd.DataFrame(cm)

        # Save accuracy to Snowflake
        # Ensure all columns are explicitly named with strings
        accuracy_df.columns = ["metric", "value"]
        report_df.columns = ["class", "precision", "recall", "f1-score", "support"]
        cm_df.columns = [f"col_{i}" for i in range(cm_df.shape[1])]  # Naming columns col_0, col_1, ...
        # Save accuracy to Snowflake with explicit column types
        accuracy_snowpark_df = session.create_dataframe(accuracy_df, schema=["metric STRING", "value FLOAT"])
        accuracy_snowpark_df.write.mode("overwrite").save_as_table("MODEL_ACCURACY")

        # Save classification report to Snowflake with explicit column types
        report_snowpark_df = session.create_dataframe(report_df, schema=[
            "class STRING", "precision FLOAT", "recall FLOAT", "f1-score FLOAT", "support FLOAT"])
        report_snowpark_df.write.mode("overwrite").save_as_table("CLASSIFICATION_REPORT")

        # Save confusion matrix to Snowflake with explicit column types
        cm_snowpark_df = session.create_dataframe(cm_df, schema=[f"col_{i} FLOAT" for i in range(cm_df.shape[1])])
        cm_snowpark_df.write.mode("overwrite").save_as_table("CONFUSION_MATRIX")
        # accuracy_snowpark_df = session.create_dataframe(accuracy_df)
        # accuracy_snowpark_df.write.mode("overwrite").save_as_table("MODEL_ACCURACY")

        # # Save classification report to Snowflake
        # report_snowpark_df = session.create_dataframe(report_df)
        # report_snowpark_df.write.mode("overwrite").save_as_table("CLASSIFICATION_REPORT")

        # # Save confusion matrix to Snowflake
        # cm_snowpark_df = session.create_dataframe(cm_df)
        # cm_snowpark_df.write.mode("overwrite").save_as_table("CONFUSION_MATRIX")

        return 'Model trained and metrics saved to Snowflake'

# Main function
def main():
    build_model()

# Call the main function
if __name__ == "__main__":
    main()