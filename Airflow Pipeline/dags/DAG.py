import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

import sys
import os

# Tambahkan folder scripts ke sys.path agar modul bisa diimport
sys.path.append(os.path.join(os.path.dirname(__file__), "../scripts"))

from extract import extract_data
from transform import transform_data
from load import load_data


default_args = {
    'owner': 'group-01',
    'start_date': dt.datetime(2024, 11, 1),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=600),
}

spark = SparkSession.builder \
    .appName("Soil-Polution-Datawarehouse-Pipeline") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

extract_parquet_path = "/opt/airflow/dags/temp/extracted"
transform_parquet_path = "/opt/airflow/dags/temp/transformed"



with DAG('P2FP',
         default_args=default_args,
         schedule_interval='10-30/10 9 * * 6',
         catchup=False,
         ) as dag:
    
    def extract_part():
        df = extract_data('/opt/airflow/dags/soil_pollution_diseases.csv', spark)
        #simpan dalam parquet (tempat menaruh file sementara), setiap kali akan di overwrite agar tidak menumpuk
        df.write.parquet(extract_parquet_path, mode="overwrite")
    
    def transform_part():
        df = spark.read.parquet(extract_parquet_path)
        df_transformed = transform_data(df, spark)
        table_name_list = []
        for key in df_transformed:
            df_transformed[key].write.parquet(transform_parquet_path+key,mode="overwrite")

        #df_transformed.write.parquet(transform_parquet_path, mode="overwrite") 
    
    def load_part():
        table_list = ['Pollutant', 'Soil', 'Farm', 'Disease', 'Case']
        for table in table_list:
            df = spark.read.parquet(transform_parquet_path+table)
            load_data(df,table)    

        # for table in table_name_list:
        #     df = spark.read.parquet(transform_parquet_path+table)
        #     load_data(df,table)

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_part
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_part
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_part
    )
    

extract >> transform >> load