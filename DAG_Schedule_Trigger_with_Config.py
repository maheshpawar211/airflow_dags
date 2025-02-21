from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

with DAG(
    'backfill_dynamic_dag',
    default_args=default_args,
    description='Backfill a target DAG using parameters passed via dag_run.conf',
    schedule_interval=None,  # Set to None for manual triggering
    start_date=days_ago(1),
    catchup=False,
) as dag:

    trigger_backfill = BashOperator(
        task_id='trigger_backfill',
        bash_command=(
            'airflow dags backfill '
            '--start-date {{ dag_run.conf.get("start_date") }} '
            '--end-date {{ dag_run.conf.get("end_date") }} '
            '{{ dag_run.conf.get("target_dag") }}'
        ),
    )

    trigger_backfill
