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
from snowflake.ml.registry import model_registry
from snowflake.ml.registry import Registry

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
session.add_packages("scikit-learn", "pandas", "snowflake-ml-python")

# Create the Snowflake table
data_base = "MITSUI_DEV"
schema_model = "MODELS"
sql_commands = create_model_stage(data_base, schema_model)
for command in sql_commands:
    execute_sql(conn, command)

def create_age_model_top(session: snowpark.Session): 

    @sproc(name="create_age_model", is_permanent=True, stage_location="@model_stage", replace=True)
    def create_age_model(session: Session) -> str:
        
        # import snowflake.snowpark as snowpark
        # from snowflake.snowpark import Session
        # from snowflake.snowpark.functions import sproc
        # import pandas as pd
        # import re #regex
        # import sklearn
        # from sklearn.model_selection import train_test_split
        # from sklearn.ensemble import RandomForestClassifier
        # from sklearn.metrics import accuracy_score, classification_report
        # import snowflake.connector
        # import os
        # from snowflake.ml.registry import model_registry

        # Snowflake Queries
        def query_data(session, query):
            """
            Executes a SQL query on the Snowflake connection and returns the results.
            """
            result = session.sql(query)
            return result.to_pandas()

        def get_age_feature_data(session):
            """
            Fetches trial data from Snowflake.
            """
            query_age_feature = "SELECT * FROM MITSUI_DEV.FEATURE_STORE.FEATURE_AGE"
            return query_data(session, query_age_feature)

        def get_demo_data(session):
            """
            Fetches demographic data from Snowflake.
            """
            query_demo = "SELECT * FROM MITSUI_DEV.DATASCIENCE_DEV.PATIENT_DATA"
            return query_data(session, query_demo)

        def find_patients_age(session):
            query_find_patients_age = "SELECT DISTINCT D.PATIENT_ID, D.AGE, F.MIN_AGE, F.MAX_AGE FROM MITSUI_DEV.DATASCIENCE_DEV.PATIENT_DATA D JOIN MITSUI_DEV.FEATURE_STORE.FEATURE_AGE F ON D.AGE BETWEEN F.MIN_AGE AND F.MAX_AGE"
            return query_data(session, query_find_patients_age)

        def democount(session):
            """
            Fetches accuracy
            """
            query_demo = "SELECT DISTINCT * FROM MITSUI_DEV.DATASCIENCE_DEV.PATIENT_DATA WHERE AGE >= 18"
            return query_data(session, query_demo)

        def modelcount(session):
            """
            Fetches accuracy
            """
            query_demo = "SELECT DISTINCT * FROM MITSUI_DEV.MODELS.FEATURE_AGE_PATIENTS"
            return query_data(session, query_demo)
        
        # Helpers
        import pandas as pd

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

        # Fetch trial and demographic data
        #conn = connect_to_snowflake()
        demo_data = get_demo_data(session)
        age_feature = get_age_feature_data(session)
        age_patients = find_patients_age(session)

        # Create pandas DataFrames from the results
        patient_df = pd.DataFrame(demo_data, columns=['PATIENT_ID', 'AGE'])
        trial_df = pd.DataFrame(age_feature, columns=['TRIAL_ID', 'MIN_AGE', 'MAX_AGE'])

        # Create a new column in the patient DataFrame indicating whether the patient is eligible for each trial
        for index, trial in trial_df.iterrows():
            trial_id = trial['TRIAL_ID']
            min_age = trial['MIN_AGE']
            max_age = trial['MAX_AGE']
            # Create a new column indicating eligibility for the specific trial
            patient_df[f'trial_{trial_id}_eligible'] = patient_df.apply(
                lambda row: 1 if pd.notna(row['AGE']) and pd.notna(min_age) and pd.notna(max_age) and row['AGE'] >= min_age and row['AGE'] <= max_age else 0,
                axis=1
            )
            # patient_df[f'trial_{trial.trial_id}_eligible'] = patient_df.apply(
            #     lambda row: 1 if pd.notna(row['AGE']) and pd.notna(trial['MIN_AGE']) and pd.notna(trial['MAX_AGE']) and row['AGE'] >= trial['MIN_AGE'] and row['AGE'] <= trial['MAX_AGE'] else 0,
            #     #lambda row: 1 if row.age >= trial.age_min and row.age <= trial.age_max else 0,
            #     axis=1
            # )

        # Define the features and target variable
        features = ['AGE']
        targets = [f'trial_{trial.TRIAL_ID}_eligible' for trial in trial_df.itertuples()]

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
            new_data = pd.DataFrame({'AGE': [25]})
            new_prediction = model.predict(new_data)
            print(f"Predicted eligibility for trial {target}: {new_prediction[0]}")

            # Register the model
            model_name = f"age_eligibility_model_{target}"
            model_version = "one"
            
            # Create the registry object
            registry = Registry(session=session)
            
            #registry = model_registry.ModelRegistry()
            # registry.log_model(
            #     model="model",
            #     model_name=model_name,
            #     model_version=model_version,
            #     flavor="sklearn",
            #     metadata={
            #         "accuracy": accuracy,
            #         "target": target,
            #         "features": features
            #     }
            # )

            registry.log_model(model,
                   model_name=model_name,
                   #version_name=model_version,
                   #conda_dependencies=["scikit-learn"],
                   comment="Mitsui test age model",
                   #metrics={"score": 96},
                   sample_input_data=X_train) #,
                   #task=type_hints.Task.TABULAR_BINARY_CLASSIFICATION)
            
            print(f"Model {model_name} version {model_version} registered successfully.")

    return "Models created and registered successfully"

def run_model_creation():
    conn = connect_to_snowflake()

    # def create_snowpark_session():
    #     conn = connect_to_snowflake()
    #     session = Session.builder.configs({
    #         "connection": conn,
    #         "account": os.environ["SNOWFLAKE_ACCOUNT"],
    #         "user": os.environ["SNOWFLAKE_USER"],
    #         "private_key_path": os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"],
    #         "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    #         "role": os.environ["SNOWFLAKE_ROLE"],
    #         "database": os.environ["SNOWFLAKE_DATABASE"],
    #         "schema": os.environ["SNOWFLAKE_SCHEMA"]
    #     }).create()

    #     return session

    # session = create_snowpark_session()
    
    create_age_model_top(session)

    print("Success")

if __name__ == "__main__":
    run_model_creation()