from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import DagRun
from airflow.utils.db import provide_session
from datetime import datetime, timedelta

# Function to fetch and print DAG executions for a specific date
@provide_session
def fetch_dag_runs(execution_date, session=None):
    formatted_date = execution_date.strftime('%Y-%m-%d')
    
    dag_runs = session.query(DagRun).filter(
        DagRun.execution_date.between(f"{formatted_date} 00:00:00", f"{formatted_date} 23:59:59")
    ).all()

    if not dag_runs:
        print(f"No DAG runs found for {formatted_date}")
    else:
        for dag_run in dag_runs:
            print(f"DAG: {dag_run.dag_id}, Execution Date: {dag_run.execution_date}, State: {dag_run.state}")

# Default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='fetch_dag_executions',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['monitoring'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_dag_runs',
        python_callable=fetch_dag_runs,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    fetch_task
