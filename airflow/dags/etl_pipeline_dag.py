from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def run_script(script):
    subprocess.run(["python", f"./{script}"], check=True)

with DAG(
    dag_id='covid_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for COVID data',
    schedule_interval='@daily',  # You can change this to @weekly if preferred
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=run_script,
        op_args=['../scripts/extract_covid_data.py']
    )

    clean_validate = PythonOperator(
        task_id='clean_and_validate',
        python_callable=run_script,
        op_args=['../transform/1_clean_validate.py']
    )

    create_dimensions = PythonOperator(
        task_id='create_dimensions',
        python_callable=run_script,
        op_args=['../transform/2_create_dimensions.py']
    )

    create_fact = PythonOperator(
        task_id='create_fact_table',
        python_callable=run_script,
        op_args=['../transform/3_create_fact_table.py']
    )

    load_postgres = PythonOperator(
        task_id='load_to_postgres',
        python_callable=run_script,
        op_args=['../scripts/load_to_postgres.py']
    )

    extract >> clean_validate >> create_dimensions >> create_fact >> load_postgres
