from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.email import EmailOperator



default_args = {
    "owner": "stellar",
    "start_date": datetime(2024, 2, 5),
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}

# Define the S3 bucket and file details
s3_prefix = "s3://airflow-snow/data/UK_Lunar_Data.csv"
s3_bucket = None


with DAG("s3_snowflake_etl",
        default_args=default_args,
        schedule_interval = "@daily",
        catchup=False
) as dag:

    
    t1 = S3KeySensor(
        task_id="file_in_s3_available",
        bucket_key=s3_prefix,
        bucket_name=s3_bucket,
        aws_conn_id="aws_s3_conn",
        wildcard_match=False,  
        poke_interval=3,  
    
    )
    

    t2 = SnowflakeOperator(
        task_id = "create_snowflake_table",
        snowflake_conn_id = "conn_id_snowflake",           
        sql = """
            DROP TABLE IF EXISTS full_moon_time;
            CREATE TABLE IF NOT EXISTS full_moon_time(
                Date VARCHAR,
                MoonriseEarly TIME,
                Moonset TIME,
                MoonriseLate TIME,
                Phase VARCHAR,
                PhaseTime TIME,
                CivilDawn TIME,
                CivilDusk TIME 	
            )
        """
    )
    

    
    t3 = SnowflakeOperator(
        task_id = "copy_csv_into_snowflake_table",
        snowflake_conn_id = "conn_id_snowflake",
        sql = """
            COPY INTO rabbit_mission.lunar.full_moon_time FROM @rabbit_mission.lunar.s3_stage FILE_FORMAT = csv_format
        """
    
    )



    t4 = EmailOperator(
        task_id="notify_by_email",
        to=["stellar.p@gmail.com"],
        subject="Loaded data into snowflake successfully on {{ ds }}",
        html_content="Your pipeline has loaded data into snowflake successfully",
    
    )


    t1 >> t2 >> t3 >> t4