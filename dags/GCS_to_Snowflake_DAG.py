from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
import os
import io



# set key credentials file path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./credentials/us-stock-market-2020-to-2024-a2e9037800f7.json"



# bucket and destination GCS
bucket_name = "stock_market_us"
destination_row_data_file_name = "row/Row-Data-US-Stock-Market-2020-to-2024.csv"
destination_transform_data_file_name = "transform/Transform-Data-US-Stock-Market-2020-to-2024.csv"



def convert_date(date_str):
    try:
        
        return pd.to_datetime(date_str, format='%d-%m-%Y')
    
    except ValueError:
        
        return pd.to_datetime(date_str, format='%d/%m/%Y')



def transform_data():
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_row_data_file_name)
    data = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(data))
        
    df.drop(columns=["Unnamed: 0"], inplace=True)
    
    df["Date"] = df["Date"].apply(convert_date)

    for i in ["Natural_Gas_Price","Natural_Gas_Vol.","Crude_oil_Price","Crude_oil_Vol.","Copper_Price","Copper_Vol.","Bitcoin_Price","Bitcoin_Vol.",
              "Platinum_Price","Platinum_Vol.","Ethereum_Price","Ethereum_Vol.","S&P_500_Price","Nasdaq_100_Price","Nasdaq_100_Vol.","Apple_Price","Apple_Vol.",
              "Tesla_Price","Tesla_Vol.","Microsoft_Price","Microsoft_Vol.","Silver_Price","Silver_Vol.","Google_Price","Google_Vol.","Nvidia_Price","Nvidia_Vol.",
              "Berkshire_Price","Berkshire_Vol.","Netflix_Price","Netflix_Vol.","Amazon_Price","Amazon_Vol.","Meta_Price","Meta_Vol.","Gold_Price","Gold_Vol."]:
    
        try:
            
            df[f"{i}"] = df[f"{i}"].astype(float).round(2)
        
        
        except ValueError:
            
            
            df[f"{i}"] = df[f"{i}"].str.replace(',', '').astype(float).round(2)


    # save csv to GCS
    blob = bucket.blob(destination_transform_data_file_name)
    df.to_csv("Transform-Data-US-Stock-Market-2020-to-2024.csv", index=False)
    blob.upload_from_filename("Transform-Data-US-Stock-Market-2020-to-2024.csv")
    
            



default_args = {
    "owner": "stellar",
    "start_date": datetime(2024, 2, 5),
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}



with DAG("gcs_snowflake_etl",
        default_args=default_args,
        schedule_interval = "@daily",
        catchup=False
) as dag:

    
    t1 = GCSObjectExistenceSensor(
        task_id= "gcs_object_exists",
        google_cloud_conn_id ="gcp_conn_id",
        bucket= bucket_name,
        object= destination_row_data_file_name,
        mode= "poke",
    
    )
    

    t2 = PythonOperator(
        task_id= "transform_data",
        python_callable= transform_data,
    )


    t3 = SnowflakeOperator(
            task_id = "create_snowflake_database",
            snowflake_conn_id = "conn_id_snowflake",           
            sql = """
                    CREATE DATABASE IF NOT EXISTS Project_US_Stock_Market ;
            """
    )
    
    
    t4 = SnowflakeOperator(
            task_id = "create_snowflake_schema",
            snowflake_conn_id = "conn_id_snowflake",           
            sql = """
                    CREATE SCHEMA IF NOT EXISTS Stock_Market ;
            """
        )


    t5 = SnowflakeOperator(
        task_id = "create_snowflake_table",
        snowflake_conn_id = "conn_id_snowflake",           
        sql = """
                CREATE TABLE IF NOT EXISTS US_Stock_Market_2020_To_2024(
                    Date datetime,
                    Natural_Gas_Price float,       
                    "Natural_Gas_Vol." float,       
                    Crude_oil_Price float,       
                    "Crude_oil_Vol." float,       
                    Copper_Price float,       
                    "Copper_Vol." float,       
                    Bitcoin_Price float,       
                    "Bitcoin_Vol." float,       
                    Platinum_Price float,       
                    "Platinum_Vol." float,       
                    Ethereum_Price float,       
                    "Ethereum_Vol." float,       
                    "S&P_500_Price" float,     
                    "Nasdaq_100_Price" float,       
                    "Nasdaq_100_Vol." float,       
                    Apple_Price float,       
                    "Apple_Vol." float,      
                    Tesla_Price float,       
                    "Tesla_Vol." float,       
                    Microsoft_Price  float,       
                    "Microsoft_Vol." float,       
                    Silver_Price float,       
                    "Silver_Vol." float,      
                    Google_Price float,     
                    "Google_Vol." float,      
                    Nvidia_Price float,      
                    "Nvidia_Vol." float,     
                    Berkshire_Price float,    
                    "Berkshire_Vol." float,     
                    Netflix_Price float,       
                    "Netflix_Vol." float,      
                    Amazon_Price float,       
                    "Amazon_Vol." float,      
                    Meta_Price float,      
                    "Meta_Vol." float,      
                    Gold_Price float,      
                    "Gold_Vol." float	
            );
        """
    )
    

    t6 = SnowflakeOperator(
        task_id = "create_snowflake_external_stage",
        snowflake_conn_id = "conn_id_snowflake",           
        sql = """
                CREATE STORAGE INTEGRATION GCS_Stage
                    TYPE = EXTERNAL_STAGE
                    STORAGE_PROVIDER = GCS
                    ENABLED = TRUE
                    STORAGE_ALLOWED_LOCATIONS  = ('gcs://stock_market_us/transform/Transform-Data-US-Stock-Market-2020-to-2024.csv');
                                    
        """
    )


    t7 = SnowflakeOperator(
        task_id = "create_snowflake_file_format",
        snowflake_conn_id = "conn_id_snowflake",           
        sql = """
                CREATE OR REPLACE file format csv_format type = 'csv' compression = 'auto' 
                    field_delimiter = ',' record_delimiter = '\n'
                    skip_header = 1 trim_space = false;
                                    
        """
    )


    t8 = SnowflakeOperator(
        task_id = "load_data_into_the_table",
        snowflake_conn_id = "conn_id_snowflake",           
        sql = """
                COPY INTO US_Stock_Market_2020_To_2024 from @GCS_Stage file_format=csv_format;

        """
    )


    t9 = EmailOperator(
        task_id="notify_by_email",
        to=["stellar.p@gmail.com"],
        subject="Loaded data into snowflake successfully on {{ ds }}",
        html_content="Your pipeline has loaded data into snowflake successfully",
    
    )



t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9