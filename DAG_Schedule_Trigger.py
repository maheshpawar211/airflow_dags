from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

# Replace these values with your target DAG and desired date range
TARGET_DAG_ID = 'your_target_dag'
BACKFILL_START_DATE = '2023-01-01'
BACKFILL_END_DATE = '2023-01-05'

with DAG(
    'backfill_target_dag',
    default_args=default_args,
    description='A DAG to backfill a specified target DAG for a given date range',
    schedule_interval=None,  # Set to None to allow manual triggering only
    start_date=days_ago(1),
    catchup=False,
) as dag:

    trigger_backfill = BashOperator(
        task_id='trigger_backfill',
        bash_command=(
            f'airflow dags backfill --start-date {BACKFILL_START_DATE} '
            f'--end-date {BACKFILL_END_DATE} {TARGET_DAG_ID}'
        ),
    )

    trigger_backfill
