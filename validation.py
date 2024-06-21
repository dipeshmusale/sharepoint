import os
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL pipeline to load data from Excel to SQL Server',
    schedule_interval=None,
    start_date=days_ago(1),
)

# Path to the Excel file
excel_file_path = '/path/to/your/file.xlsx'

# Database connection parameters
db_connection_string = 'mssql+pymssql://username:password@hostname:port/database'

def check_if_excel(file_path):
    if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
        return True
    else:
        raise ValueError("The provided file is not an Excel file.")

def check_if_data_present(file_path):
    df = pd.read_excel(file_path)
    if df.empty:
        raise ValueError("The Excel file is empty.")
    else:
        return True

def extract_and_load(file_path, db_conn_str):
    df = pd.read_excel(file_path)
    engine = create_engine(db_conn_str)
    df.to_sql('your_table_name', engine, if_exists='replace', index=False)

# Define tasks
task_check_if_excel = PythonOperator(
    task_id='check_if_excel',
    python_callable=check_if_excel,
    op_args=[excel_file_path],
    dag=dag,
)

task_check_if_data_present = PythonOperator(
    task_id='check_if_data_present',
    python_callable=check_if_data_present,
    op_args=[excel_file_path],
    dag=dag,
)

task_extract_and_load = PythonOperator(
    task_id='extract_and_load',
    python_callable=extract_and_load,
    op_args=[excel_file_path, db_connection_string],
    dag=dag,
)

# Define task dependencies
task_check_if_excel >> task_check_if_data_present >> task_extract_and_load
