import pandas as pd 
from sqlalchemy import create_engine, text, Text
from sqlalchemy.dialects.postgresql import ARRAY
from dotenv import load_dotenv
import os

load_dotenv()
POSTGRES_URI = os.getenv("POSTGRES_URI")
engine = create_engine(POSTGRES_URI)

# Function to read the data from the database and store it into a dataframe
def read_data_and_store(ott_name: str):
    with engine.begin() as conn:
        df = pd.read_sql(
            f"raw_{ott_name}_titles",
            con=conn,
            index_col="show_id"
        )

        return df

# transformation 1: changes the cast column into an ARRAY of names
def transform_cast_into_array(df_name):
    df_name["cast"] =  df_name["cast"].map(lambda x: str(x).split(","))
    df_name["cast"] = df_name["cast"].map(lambda x : [items.strip() for items in x])

# transformation 2: changes the listed_in colum into an ARRAY
def transform_listed_in_into_array(df_name):
    df_name["listed_in"] =  df_name["listed_in"].map(lambda x: str(x).split(","))
    df_name["listed_in"] = df_name["listed_in"].map(lambda x : [items.strip() for items in x])

# Function to save the transformed data into another table
def save_data_into_table(ott_name: str, df):
    with engine.begin() as conn:
        query = text(f"""
                        CREATE TABLE IF NOT EXISTS {ott_name}_titles(
                        show_id VARCHAR(20) PRIMARY KEY NOT NULL,
                        type TEXT NOT NULL,
                        title TEXT NOT NULL,
                        director TEXT,
                        "cast" TEXT[],
                        country TEXT,
                        date_added TEXT, 
                        release_year INT,
                        rating TEXT,
                        duration TEXT,
                        listed_in TEXT[], 
                        description TEXT
                    )
                    """)
        conn.execute(query)

        df.to_sql(
            con=conn,
            name=f'{ott_name}_titles',
            if_exists='replace',
            index_label='show_id',
            dtype={
                "cast" : ARRAY(Text),
                "listed_in" : ARRAY(Text)
            }
        )

# Function to create the master table
def create_master_table():
    with engine.begin() as conn:
        query = text(f"""
                        CREATE VIEW master_table as (
                            SELECT 
                                *,
                                'netflix' as platform
                            FROM netflix_titles

                            UNION ALL

                            SELECT 
                                *,
                                'disney_plus' as platform
                            FROM disney_plus_titles

                            UNION ALL

                            SELECT 
                                *,
                                'amazon_prime' as platform
                            FROM amazon_prime_titles

                            UNION ALL

                            SELECT 
                                *,
                                'hulu' as platform
                            FROM hulu_titles

                        )
                    """)
        conn.execute(query)

# creating indivisual dataframes for each OTT platforms
netflix_df = read_data_and_store('netflix')
amazon_df = read_data_and_store('amazon_prime')
disney_df = read_data_and_store('disney_plus')
hulu_df = read_data_and_store('hulu')

# applying transformation 1
transform_cast_into_array(netflix_df)
transform_cast_into_array(amazon_df)
transform_cast_into_array(disney_df)
transform_cast_into_array(hulu_df)

# applying transformation 2
transform_listed_in_into_array(netflix_df)
transform_listed_in_into_array(amazon_df)
transform_listed_in_into_array(disney_df)
transform_listed_in_into_array(hulu_df)

# Saving data into sql tables
save_data_into_table('netflix', netflix_df)
save_data_into_table('amazon_prime', amazon_df)
save_data_into_table('disney_plus', disney_df)
save_data_into_table('hulu', hulu_df)

# create the master table 
create_master_table()
