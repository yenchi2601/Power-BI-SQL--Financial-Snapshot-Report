import os
import pyodbc
import pandas as pd
import numpy as np
import re
import logging
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from tqdm import tqdm

# Set the display option to show all columns
pd.options.display.max_columns = None

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Centralized SQL file directory
SQL_FILE_DIR = os.path.dirname(os.path.abspath(__file__))

def get_sql_file_path(file_name):
    return os.path.join(SQL_FILE_DIR, file_name)

# Function to connect to Azure SQL Database
def connect_to_azure_sql(server, database, username, password):
    try:
        connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password}"
        )
        connection = pyodbc.connect(connection_string)
        logging.info("Connected to Azure SQL Database.")
        return connection
    except Exception as e:
        logging.error(f"Error connecting to Azure SQL Database: {e}")
        raise

# Function to connect to PostgreSQL
def connect_to_postgres(host, database, username, password):
    try:
        connection = psycopg2.connect(
            host=host,
            dbname=database,
            user=username,
            password=password
        )
        logging.info("Connected to PostgreSQL Database.")
        return connection
    except Exception as e:
        logging.error(f"Error connecting to PostgreSQL Database: {e}")
        raise

# Function to load SQL query from file
def load_query_from_file(file_name):
    try:
        file_path = get_sql_file_path(file_name)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File '{file_name}' not found at {file_path}.")
        with open(file_path, 'r') as file:
            return file.read()
    except Exception as e:
        logging.error(f"Error loading SQL query from file '{file_name}': {e}")
        raise

# Fetch data as DataFrame with alias name correction
def fetch_data_as_dataframe(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        
        # Get column names **with aliases applied in the SQL query**
        column_names = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=column_names)

        if df.empty:
            logging.warning("Query returned no data.")
        return df

    except Exception as e:
        logging.error(f"Error executing query: {e}")
        raise


# Function to load and execute SQL queries
def load_and_fetch_data(connection, sql_files):
    dataframes = {}
    for sql_file in sql_files:
        query = load_query_from_file(sql_file)
        df_name = sql_file.split('.')[0]
        dataframes[df_name] = fetch_data_as_dataframe(connection, query)
        logging.info(f"DataFrame '{df_name}' created with {len(dataframes[df_name])} rows.")
    return dataframes

# Function to rename columns in a DataFrame based on a dictionary
def rename_columns(df, rename_dict):
    return df.rename(columns=rename_dict)


# Create Table If Not Exists
def create_table_if_not_exists(engine):
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'weekly_snapshot' AND TABLE_SCHEMA = 'dbo')
    BEGIN
        CREATE TABLE [dbo].[weekly_snapshot] (
            [date]                 DATE           NOT NULL,
            [year]                 INT            NULL,
            [month]                INT            NULL,
            [day]                  INT            NULL,
            [day_name]             VARCHAR (10)   NULL,
            [month_name]           VARCHAR (10)   NULL,
            [week_number]          INT            NULL,
            [financial_year]       VARCHAR (50)   NULL,
            [financial_week]       INT            NULL,
            [MTD]                  INT            NULL,
            [FYTD]                 INT            NULL,
            [clinic_key]           VARCHAR (10)   NULL,
            [clinic]               VARCHAR (100)  NULL,
            [clinic_description]   VARCHAR (100)  NULL,
            [modality_key]         VARCHAR (10)   NULL,
            [modality]             NVARCHAR (100) NULL,
            [modality_description] VARCHAR (100)  NULL,
            [budget_exams]         FLOAT (53)     NULL,
            [budget_revenue]       FLOAT (53)     NULL,
            [exams]                FLOAT (53)     NULL,
            [revenue]              FLOAT (53)     NULL,
            [exams_prior]          FLOAT (53)     NULL,
            [revenue_prior]        FLOAT (53)     NULL,
            [public_holiday]       VARCHAR (50)   NULL
        );
    END
    """
    with engine.begin() as conn:
        conn.execute(text(create_table_query))
        print("Table checked/created successfully.")

# Ensure data types in final_df match the Azure SQL table schema
def adjust_data_types(final_df):
    # Convert numeric columns to FLOAT to match the Azure SQL schema
    numeric_columns = ['budget_exams', 'budget_revenue', 'exams', 'revenue', 'exams_prior', 'revenue_prior']
    final_df[numeric_columns] = final_df[numeric_columns].astype(float)
    
    # Convert 'date' column to match SQL 'DATE' format (YYYY-MM-DD)
    final_df['date'] = final_df['date'].dt.strftime('%Y-%m-%d')
    
    # Optionally log the data types after adjustment for verification
    logging.info("Adjusted data types in final_df:")
    logging.info(final_df.dtypes)
    
    return final_df

# Function to insert DataFrame into Azure SQL
def insert_to_azure(final_df):
    
    # Fetch credentials from environment variables
    azure_prod_server = os.getenv("AZURE_SQL_SERVER")
    azure_prod_database = os.getenv("AZURE_SQL_DATABASE")
    azure_prod_username = os.getenv("AZURE_SQL_USERNAME")
    azure_prod_password = os.getenv("AZURE_SQL_PASSWORD")
        
    # Ensure the DataFrame is not empty
    if final_df.empty:
        print("Final_df is empty. No data to insert.")
        logging.error("final_df is empty. No data to insert into Azure SQL.")
        return
    
    # Log the size and column types of final_df
    print(f"DataFrame has {len(final_df)} rows and {len(final_df.columns)} columns.")
    logging.info(f"DataFrame has {len(final_df)} rows and {len(final_df.columns)} columns.")
    logging.info(f"Data types in final_df: {final_df.dtypes}")
    # Construct the connection string
    conn_str = f"mssql+pyodbc://{azure_prod_username}:{azure_prod_password}@{azure_prod_server}/{azure_prod_database}?driver=ODBC+Driver+17+for+SQL+Server"

    # Create SQLAlchemy engine
    engine = create_engine(conn_str)

    # Define the table name in Azure SQL
    ##table_name = "dbo.weekly_snapshot"  # Replace with the actual table name
    schema_name = "dbo"
    table_name = "weekly_snapshot"
    # Truncate the table before inserting new data
    with engine.begin() as conn:
        try:
            print(f"Deleting existing data from {schema_name}.{table_name}...")
            conn.execute(text(f"DELETE FROM {schema_name}.{table_name}"))
            print("Existing data deleted successfully.")
        except Exception as e:
            print(f"Error deleting data: {e}")
            logging.error(f"Error deleting data from {schema_name}.{table_name}: {e}")
            return

    # Log DataFrame info before inserting
    print(f"Inserting data into table {schema_name}.{table_name}...")
    logging.info(f"Inserting {len(final_df)} rows into {schema_name}.{table_name}.")
    logging.info(f"Columns in final_df: {', '.join(final_df.columns)}")

    # Insert the DataFrame into Azure SQL with a progress bar
    try:
        # Set chunksize for efficient data insertion
        chunksize = 1000  # Adjust this as needed based on your data size
        
        # Number of rows to insert (for progress bar calculation)
        total_rows = len(final_df)
        
        # Create a progress bar using tqdm
        with tqdm(total=total_rows, desc="Inserting data", unit="rows", bar_format="{l_bar}{bar} {n_fmt}/{total_fmt} [{percentage:.1f}%]") as pbar:
            # Insert data in chunks
            for start in range(0, len(final_df), chunksize):
                chunk = final_df.iloc[start:start + chunksize].copy()
                chunk.to_sql(table_name, engine, if_exists="append", index=False, chunksize=chunksize, schema=schema_name)
                # Update the progress bar
                pbar.update(len(chunk))
        print("Data inserted successfully into azure_prod!")
        logging.info("Data inserted successfully into azure_prod!")

    except Exception as e:
        print(f"Error inserting data into Azure SQL: {e}")
        logging.error(f"Error inserting data into Azure SQL: {e}")
# Main execution
def main():
    # Fetch credentials from environment variables
    azure_credentials = {
        "server": os.getenv("AZURE_SQL_SERVER"),
        "database": os.getenv("AZURE_SQL_DATABASE"),
        "username": os.getenv("AZURE_SQL_USERNAME"),
        "password": os.getenv("AZURE_SQL_PASSWORD")
    }
    postgres_credentials = {
        "host": os.getenv("POSTGRES_HOST"),
        "database": os.getenv("POSTGRES_DATABASE"),
        "username": os.getenv("POSTGRES_USERNAME"),
        "password": os.getenv("POSTGRES_PASSWORD")
    }
    """
    azure_dev_credentials = {
        "server": os.getenv("AZURE_DEV_SERVER"),
        "database": os.getenv("AZURE_DEV_DATABASE"),
        "username": os.getenv("AZURE_DEV_USERNAME"),
        "password": os.getenv("AZURE_DEV_PASSWORD")
    }
    """
    # Create SQLAlchemy Engine for Efficient Inserts
    engine = create_engine(
        f"mssql+pyodbc://{azure_credentials['username']}:{azure_credentials['password']}"
        f"@{azure_credentials['server']}/{azure_credentials['database']}?driver=ODBC+Driver+17+for+SQL+Server", fast_executemany=True)

    # Validate credentials
    if not all(azure_credentials.values()):
        logging.error("One or more Azure SQL credentials are missing.")
        return
    if not all(postgres_credentials.values()):
        logging.error("One or more PostgreSQL credentials are missing.")
        return
    """
    if not all(azure_dev_credentials.values()):
        logging.error("One or more Azure Dev SQL credentials are missing.")
        return
    """
    azure_sql_files = ["fetch_budget.sql", "fetch_dates.sql", "fetch_public_holidays.sql"]
    postgres_sql_files = ["fetch_current_year.sql", "fetch_prior_year.sql", "fetch_clinics.sql", "fetch_modality.sql"]
    
    try:
        # Connect to databases
        azure_conn = connect_to_azure_sql(**azure_credentials)
        postgres_conn = connect_to_postgres(**postgres_credentials)

        # Fetch data from both databases
        azure_data = load_and_fetch_data(azure_conn, azure_sql_files)
        postgres_data = load_and_fetch_data(postgres_conn, postgres_sql_files)

        # Merge DataFrames
        all_data = {**azure_data, **postgres_data}

        # Apply renaming to each DataFrame using a mapping
        rename_dicts = {
            "fetch_clinics": {"sl_key": "clinic_key", 
                              "clinic_short": "clinic"},
            "fetch_dates": {"date_value": "date"},
            "fetch_public_holidays": {"date_value": "date"},
            "fetch_modality": {"sl_key": "modality_key", 
                               "modality_short": "modality"},
            "fetch_prior_year": {"ce_start": "date",
                                 "ce_site": "clinic_key",
                                 "ex_type": "modality_key",
                                 "exams": "exams_prior",
                                 "revenue": "revenue_prior"
                                },
            "fetch_current_year": {"ce_start": "date",
                                   "ce_site": "clinic_key",
                                   "ex_type": "modality_key"
                                  },
            "fetch_budget": {"budget_exams.amount": "budget_exams",
                             "budget_revenue.amount": "budget_revenue"
                            }
        }

        for df_name, rename_dict in rename_dicts.items():
            if df_name in all_data:
                all_data[df_name] = rename_columns(all_data[df_name], rename_dict)

        # Convert all date columns to datetime once
        for df_name in all_data:
            if 'date' in all_data[df_name].columns:
                all_data[df_name]['date'] = pd.to_datetime(all_data[df_name]['date'], errors='coerce')

        # Merge all dataframes based on specific logic
        fetch_dates, fetch_current_year, fetch_prior_year = all_data["fetch_dates"], all_data["fetch_current_year"], all_data["fetch_prior_year"]
        fetch_clinics, fetch_modality, fetch_public_holidays, fetch_budget = all_data["fetch_clinics"], all_data["fetch_modality"], all_data["fetch_public_holidays"], all_data["fetch_budget"]
        
        # **Remove 29/02 from Prior Year if Current Year is NOT a Leap Year**
        current_year = fetch_current_year["date"].dt.year.iloc[0]  # Assuming all rows belong to the same year
        if not (current_year % 4 == 0 and (current_year % 100 != 0 or current_year % 400 == 0)):
            fetch_prior_year = fetch_prior_year[~((fetch_prior_year["date"].dt.month == 2) & (fetch_prior_year["date"].dt.day == 29))]
        # Increase date of prior_year by 1 year
        fetch_prior_year["date"] = fetch_prior_year["date"] + pd.DateOffset(years=1)        
        # **Remove duplicates that might have been created (especially for 28/02)**
        fetch_prior_year = fetch_prior_year.drop_duplicates(subset=["date", "clinic_key", "modality_key"])
        
        # Create all_dates DataFrame
        all_dates = pd.DataFrame({"date": pd.concat([fetch_dates["date"], fetch_current_year["date"], fetch_prior_year["date"]]).unique()})

        # 1. Join all_dates with fetch_dates to ensure full date attributes
        dates_df = pd.merge(all_dates, fetch_dates, on="date", how="left")

        # Create a cartesian product of dates and clinics
        generate_df = dates_df.merge(fetch_clinics, how='cross').merge(fetch_modality, how='cross')

        # 1.2 Join above created df with Budget Exams & Revenue Dataframe (Left Join)
        Merge1_df = generate_df.merge(fetch_budget,  on=["date", "clinic", "modality"], how="left")
        Merge1_df = Merge1_df[['date','year','month','day','day_name','month_name','week_number','financial_year', 'financial_week', 'MTD', 'FYTD', 
                           'clinic_key', 'clinic', 'clinic_description','modality_key', 'modality','modality_description','budget_exams', 'budget_revenue']]
        logging.info('Printing merge 1 dataframe:')
        logging.info(Merge1_df)
 
        # 2 Join fetch_dates with Current Year Dataframe (Left Join)
        Merge2_df = Merge1_df.merge(fetch_current_year, on=["date", "clinic_key", "modality_key"], how="left")
    
        Merge2_df['exams'] = Merge2_df['exams'].fillna(0).astype(int)
        Merge2_df['revenue'] = Merge2_df['revenue'].fillna(0).astype(float)

        logging.info('Printing merge 2 dataframe:')
        logging.info(Merge2_df)

   
        # 3. Join with Prior Year dataframe (left join)
        Merge3_df = Merge2_df.merge(fetch_prior_year, 
                                    on=["date", "clinic_key", "modality_key"], 
                                    how="left" 
                                   )
        logging.info('Printing merge 3 dataframe:')
        logging.info(Merge3_df)

        # 5. Join above created dataframe with Public Holidays dataframe (Left Join)
        fetch_public_holidays['date'] = pd.to_datetime(fetch_public_holidays['date'], errors='coerce')
        final_df = Merge3_df.merge(fetch_public_holidays, on="date", how="left")

        # 6. Fill missing values with default values and handle text columns
        final_df[['year', 'month', 'day', 'week_number', 'financial_week', 'MTD', 'FYTD', 'exams', 'exams_prior']] = final_df[['year', 'month', 'day', 'week_number', 'financial_week', 'MTD', 'FYTD', 'exams', 'exams_prior']].fillna(0).astype(int)
        final_df[['revenue', 'revenue_prior', 'budget_revenue', 'budget_exams']] = final_df[['revenue', 'revenue_prior', 'budget_revenue', 'budget_exams']].fillna(0).astype(float)
        final_df[['clinic_key', 'modality_key', 'modality', 'modality_description', 'clinic', 'clinic_description', 'public_holiday']] = final_df[['clinic_key', 'modality_key', 'modality', 'modality_description', 'clinic', 'clinic_description', 'public_holiday']].replace({pd.NA: None, np.nan: None})
        
        logging.info('Printing final dataframe:')
        final_df = final_df.replace({np.nan: None})
        logging.info(final_df)
   
        print("DataFrame Columns:", final_df.columns.tolist())  
       
        print("DataFrame Data Types:\n", final_df.dtypes)  
        """
        # TESTING DF 
            # Define start and end dates
            start_date = "2024-02-29"
            end_date = "2024-02-29" 
        # Filter DataFrame
            filtered_df = final_df[
            (final_df['date'] >= start_date) & 
            (final_df['date'] <= end_date) & 
            (final_df['clinic'] == "ASHFORD") &
            (final_df['modality'] == "ULSOUND")
        ]
        # Print the filtered DataFrame
            logging.info('Printing final dataframe of ASH:')
            logging.info(' ')
            print(filtered_df)
        """    
        # Save to CSV
        #final_df.to_csv('test_final.csv', index=False)
        create_table_if_not_exists(engine)

        # Insert the final_df into azure_dev database
        logging.info(f"Rows in final_df before insertion: {len(final_df)}")
        insert_to_azure(final_df)
        
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        # Close connections
        if 'azure_conn' in locals():
            azure_conn.close()
            logging.info("Azure SQL Connection closed.")
        if 'postgres_conn' in locals():
            postgres_conn.close()
            logging.info("PostgreSQL Connection closed.")


if __name__ == "__main__":
    main()

