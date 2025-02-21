from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagRun
from airflow.utils.state import State
from airflow.utils import timezone
from airflow.settings import Session
from datetime import datetime, timedelta

def trigger_backfill(**kwargs):
    # Read the configuration passed during the DAG run
    dag_conf = kwargs.get("dag_run").conf or {}
    target_dag_id = dag_conf.get("target_dag")
    start_date_str = dag_conf.get("start_date")
    end_date_str = dag_conf.get("end_date")
    
    if not (target_dag_id and start_date_str and end_date_str):
        raise ValueError("Missing required parameters: 'target_dag', 'start_date', and 'end_date' are needed.")

    # Parse the provided dates (expected format: YYYY-MM-DD)
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    
    session = Session()
    current_date = start_date
    while current_date <= end_date:
        run_id = f"manual__{current_date.isoformat()}"
        # Check if a DagRun for this execution_date already exists (optional safeguard)
        existing_run = session.query(DagRun).filter(
            DagRun.dag_id == target_dag_id,
            DagRun.execution_date == current_date
        ).first()
        if not existing_run:
            dr = DagRun(
                dag_id=target_dag_id,
                run_id=run_id,
                execution_date=current_date,
                start_date=timezone.utcnow(),
                state=State.RUNNING,
                external_trigger=True,
            )
            session.add(dr)
            session.commit()
            print(f"Triggered DAG run for {target_dag_id} on {current_date}")
        else:
            print(f"DAG run for {target_dag_id} on {current_date} already exists.")
        current_date += timedelta(days=1)

with DAG(
    'backfill_dynamic_dag_python',
    description='Programmatically trigger backfill runs for a target DAG using dag_run.conf parameters',
    schedule_interval=None,  # Manual trigger only
    start_date=timezone.utcnow(),
    catchup=False,
    default_args={'owner': 'airflow', 'depends_on_past': False},
    tags=['backfill'],
) as dag:

    trigger_backfill_task = PythonOperator(
        task_id='trigger_backfill_task',
        python_callable=trigger_backfill,
        provide_context=True
    )

    trigger_backfill_task
