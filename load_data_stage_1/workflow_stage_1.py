import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# loading env
load_dotenv()

# loading postgres uri
POSTGRES_URI = os.getenv('POSTGRES_URI')

# creating postgres engine
engine = create_engine(POSTGRES_URI)

# creating a constructor function that takes file name input and returns the dataframe by reading that csv file
def df_constructor_function(file_name : str):
    df = pd.read_csv(f'csv_files/{file_name}.csv')
    return df

# This function takes a OTT name and execute an SQL query that creates an SQL table if not exists and truncate the table
def create_sql_table_and_truncate_table(ott_name: str):
    with engine.begin() as conn:
        # creates a new table if not exists
        query = text(
                    f"""
                        CREATE TABLE IF NOT EXISTS {ott_name}_titles(
                        show_id VARCHAR(20) PRIMARY KEY NOT NULL,
                        type TEXT NOT NULL,
                        title TEXT NOT NULL,
                        director TEXT,
                        "cast" TEXT,
                        country TEXT,
                        date_added TEXT, 
                        release_year INT,
                        rating TEXT,
                        duration TEXT,
                        listed_in TEXT, 
                        description TEXT
                    )
                    """
                )
        conn.execute(query)

        # truncates the table 
        query = text(f"TRUNCATE TABLE {ott_name}_titles")
        conn.execute(query)

# creating different dataframes for all the OTT platform csv data 
netflix_df = df_constructor_function("netflix_titles")
amazon_prime_df = df_constructor_function("amazon_prime_titles")
disney_plus_df = df_constructor_function("disney_plus_titles")
hulu_df = df_constructor_function("hulu_titles")

create_sql_table_and_truncate_table("hulu")
create_sql_table_and_truncate_table("amazon_prime")
create_sql_table_and_truncate_table("disney_plus")
create_sql_table_and_truncate_table("netflix")
