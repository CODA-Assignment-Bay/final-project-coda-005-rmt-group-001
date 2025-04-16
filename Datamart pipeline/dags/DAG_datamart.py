import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
import pandas as pd

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

extract_parquet_path = "/opt/airflow/dags/temp/extracted"
transform_parquet_path = "/opt/airflow/dags/temp/transformed"

temp_csv_path = '/opt/airflow/dags/temp/'


with DAG('P2FP_datamart',
         default_args=default_args,
         schedule_interval='10-30/10 9 * * 6',
         catchup=False,
         ) as dag:
    
    def extract_part():
        measurement_item_df = extract_data('/opt/airflow/dags/measurement_item_info.csv')
        measurement_info_df = extract_data('/opt/airflow/dags/measurement_info.csv')
        measurement_station_df = extract_data('/opt/airflow/dags/measurement_station_info.csv')

        measurement_item_df.to_csv(temp_csv_path+'extract_measurement_item.csv',index=False)
        measurement_info_df.to_csv(temp_csv_path+'extract_measurement_info.csv',index=False)
        measurement_station_df.to_csv(temp_csv_path+'extract_measurement_station.csv',index=False)
        
       
    def transform_part():
        measurement_info_df = pd.read_csv(temp_csv_path+'extract_measurement_info.csv')

        measurement_info_df, dim_date = transform_data(measurement_info_df)

        measurement_info_df.to_csv(temp_csv_path+'transform_measurement_info.csv',index=False)
        dim_date.to_csv(temp_csv_path+'dim_date.csv',index=False)



    def load_part():
        measurement_info_df = pd.read_csv(temp_csv_path+'transform_measurement_info.csv')
        measurement_item_df = pd.read_csv(temp_csv_path+'extract_measurement_item.csv')
        measurement_station_df = pd.read_csv(temp_csv_path+'extract_measurement_station.csv')
        dim_date = pd.read_csv(temp_csv_path+'dim_date.csv')

        #load_data(measurement_station_df, measurement_item_df, measurement_info_df, dim_date, measurement_recap_df)
        load_data(measurement_station_df, measurement_item_df, measurement_info_df, dim_date)

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