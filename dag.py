from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os
import logging

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'data_ingestion_and_processing',
    default_args=default_args,
    description='A simple data ingestion and processing workflow',
    schedule_interval='@weekly',  # Run this DAG once a week
    start_date=days_ago(1),
    tags=['example'],
)

# Paths
UPLOAD_DIRECTORY = '/path/to/uploaded/files'
PROCESSED_DIRECTORY = '/path/to/processed/files'

def check_for_new_files():
    files = [f for f in os.listdir(UPLOAD_DIRECTORY) if f.endswith('.xlsx')]
    if not files:
        raise ValueError('No new files found.')
    return files

def validate_and_reformat_data(filename):
    file_path = os.path.join(UPLOAD_DIRECTORY, filename)
    try:
        df = pd.read_excel(file_path)
        
        # Validate the data format
        if 'required_column' not in df.columns:
            raise ValueError(f"File {filename} is missing required columns.")
        
        # Reformat data if necessary
        df = df.rename(columns={'old_column_name': 'new_column_name'})
        
        # Save reformatted data
        processed_file_path = os.path.join(PROCESSED_DIRECTORY, filename)
        df.to_excel(processed_file_path, index=False)
    except Exception as e:
        logging.error(f"Error processing file {filename}: {e}")
        raise

def process_files(**context):
    files = context['task_instance'].xcom_pull(task_ids='check_for_new_files')
    for file in files:
        validate_and_reformat_data(file)

# Task to check for new files
check_files_task = PythonOperator(
    task_id='check_for_new_files',
    python_callable=check_for_new_files,
    dag=dag,
)

# Task to process files
process_files_task = PythonOperator(
    task_id='process_files',
    python_callable=process_files,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
check_files_task >> process_files_task
