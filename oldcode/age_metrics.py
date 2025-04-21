from snowflake_queries import connect_to_snowflake, get_demo_data, get_age_feature_data, find_patients_age, democount, modelcount
from helpers import execute_sql, create_table, insert_data
import pandas as pd
import re #regex

def age_metrics():
    # Step 1: Connect to Snowflake
    conn = connect_to_snowflake()

    # Calculate # of people >= 18 in demo table
    demometric = democount(conn)
    demometric_df = pd.DataFrame(demometric)
    # Count the number of rows in the DataFrame
    row_count_d = demometric_df.shape[0]
    print(f"Number of rows: {row_count_d}")

    # Count rows in age feature data tablemetric = democount(conn)
    modelmetric = modelcount(conn)
    modelmetric_df = pd.DataFrame(modelmetric)
    # Count the number of rows in the DataFrame
    row_count_m = modelmetric_df.shape[0]
    print(f"Number of rows: {row_count_m}")

    # Step 3: Compare rows and calculate percentage match (if applicable)
    if row_count_d > 0 and row_count_m > 0:
        # Compare row values (assuming some comparison logic, e.g., matching on patient_id)
        matching_rows = demometric_df.merge(modelmetric_df, how ='inner')  # Merge DataFrames on common columns

        # Calculate percentage match (matching rows / total rows in demo table)
        percentage_match = (matching_rows.shape[0] / row_count_d) * 100
        print(f"Percentage match: {percentage_match:.2f}%")
    else:
        print("No matching rows found or one of the tables is empty.")

    return modelmetric_df, row_count_d, row_count_m, percentage_match if row_count_d > 0 and row_count_m > 0 else None

if __name__ == "__main__":
    age_metrics()