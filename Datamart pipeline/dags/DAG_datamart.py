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

from extract import get_data_from_db
from transform import pivot_and_rename, merge_pivot, process_summary, AQI_parameter
from load import load_data


default_args = {
    'owner': 'group-01',
    'start_date': dt.datetime(2025, 4, 16),
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
        query = '''SELECT * FROM "Dim_Item";'''
        df_item = get_data_from_db(query)
        df_item.to_csv(temp_csv_path+'extract_df_item.csv',index=False)
        
        query = '''SELECT * FROM "Dim_Station";'''
        df_station = get_data_from_db(query)
        df_station.to_csv(temp_csv_path+'extract_df_station.csv',index=False)

        query = '''SELECT * FROM "Dim_Date";'''
        df_date_sample = get_data_from_db(query)
        df_date_sample.to_csv(temp_csv_path+'extract_df_date_sample.csv',index=False)

        query = '''SELECT * FROM "Fact_Info";'''
        df_measurement_sample = get_data_from_db(query)
        df_measurement_sample.to_csv(temp_csv_path+'extract_df_measurement_sample.csv',index=False)         
        
        #measurement_info_df.to_csv(temp_csv_path+'extract_measurement_info.csv',index=False)
        #measurement_item_df = extract_data('/opt/airflow/dags/measurement_item_df.csv')

        #df_item = get_data_from_db('/opt/airflow/dags/df_item.csv''SELECT * FROM "Dim_Item";''')) ???

       
    def transform_part():

        df_measurement_sample = pd.read_csv(temp_csv_path+'extract_df_measurement_sample.csv')
        df_measurement_sample_piv = pivot_and_rename(df_measurement_sample)
        df_measurement_sample_piv.to_csv(temp_csv_path+'df_measurement_sample_piv.csv',index=False) 

        df_measurement_sample_piv = pd.read_csv(temp_csv_path+'df_measurement_sample_piv.csv')
        df_station = pd.read_csv(temp_csv_path+'extract_df_station.csv')
        df_merged = merge_pivot(df_measurement_sample_piv, df_station)
        df_merged.to_csv(temp_csv_path+'df_merged.csv',index=False)

        df_merged = pd.read_csv(temp_csv_path+'df_merged.csv')
        df_summary = process_summary(df_merged)
        df_summary.to_csv(temp_csv_path+'df_summary.csv',index=False)

        df_summary = pd.read_csv(temp_csv_path+'df_summary.csv')
        datamart_2 = AQI_parameter(df_summary)
        datamart_2.to_csv(temp_csv_path+'datamart.csv',index=False)

        #measurement_info_df = pd.read_csv(temp_csv_path+'extract_measurement_info.csv')
        #measurement_info_df, dim_date = transform_data(measurement_info_df)
        #measurement_info_df.to_csv(temp_csv_path+'transform_measurement_info.csv',index=False)
        #dim_date.to_csv(temp_csv_path+'dim_date.csv',index=False)


    def load_part():
        datamart = pd.read_csv(temp_csv_path+'datamart.csv')
        load_data(datamart)

        #measurement_info_df = pd.read_csv(temp_csv_path+'transform_measurement_info.csv')
        #measurement_item_df = pd.read_csv(temp_csv_path+'extract_measurement_item.csv')
        #measurement_station_df = pd.read_csv(temp_csv_path+'extract_measurement_station.csv')
        #dim_date = pd.read_csv(temp_csv_path+'dim_date.csv')
        #load_data(measurement_station_df, measurement_item_df, measurement_info_df, dim_date)

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