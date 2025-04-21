import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from helpers import execute_sql, create_table, insert_data, create_model_stage
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from snowflake.snowpark.functions import sproc, col, lit
from snowflake.snowpark.types import IntegerType, FloatType, BooleanType
from sklearn.ensemble import RandomForestClassifier
import snowflake.connector
from snowflake.ml.registry import Registry
import joblib

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
        "schema": "MODELS"
    }).create()

    session.add_packages(
    "scikit-learn",
    "snowflake-ml-python")

    # Define a function to train the model
    @sproc(name='train_model',
        is_permanent=True,
        stage_location="@MITSUI_DEV.MODELS.MODEL_STAGE",
        replace=True,
        packages=["snowflake-snowpark-python", "scikit-learn", "joblib", "snowflake-ml-python"],
        session=session)
    def train_model(session: Session) -> str:
        # Define the training data
        patient_age = session.table("MITSUI_DEV.FEATURE_STORE.PATIENT_AGE")
        trial_age = session.table("MITSUI_DEV.FEATURE_STORE.TRIAL_AGE")

        # Create all possible patient-trial combinations
        combinations = patient_age.cross_join(trial_age)

        # Create target variable
        combinations = combinations.with_column("is_eligible", 
            (col("AGE") >= col("MIN_AGE")) & (col("AGE") <= col("MAX_AGE")))

        # Feature engineering - add an age difference from ranges
        combinations = combinations.with_columns([
            ("age_diff_from_min", col("AGE") - col("MIN_AGE")),
            ("age_diff_from_max", col("MAX_AGE") - col("AGE"))
        ])

        # Prepare features and target
        features = ["AGE", "MIN_AGE", "MAX_AGE", "age_diff_from_min", "age_diff_from_max"]
        X = combinations.select(features)
        y = combinations.select("is_eligible")

        # Convert to pandas for scikit-learn operations (necessary for now)
        X_pd = X.to_pandas()
        y_pd = y.to_pandas()["IS_ELIGIBLE"]

        # Train a RandomForestClassifier model on the training data
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X_pd, y_pd)

        # Predict using the trained model
        predictions = model.predict(X_pd)
        
        # Create a Snowpark DataFrame for predictions
        prediction_df = session.create_dataframe(predictions, schema=["prediction"])
        prediction_df.write.mode("overwrite").save_as_table("MODEL_OUTPUT", table_type="transient")

        # Save the model to the model registry
        registry = Registry(session, database_name="MITSUI_DEV", schema_name="MODELS")
        registry.log_model(model,
                        model_name='random_forest',
                        comment='Random forest model trained on dataset',
                        conda_dependencies=['scikit-learn', 'joblib'],
                        sample_input_data=X_pd)

        return "Model trained, predictions saved, and model logged to registry"

    # Execute the stored procedure
    result = session.call('train_model')
    print(result)

    # Close the session
    session.close()

# Main function
def main():
    build_model()

# Call the main function
if __name__ == "__main__":
    main()