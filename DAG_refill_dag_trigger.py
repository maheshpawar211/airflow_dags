from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 20),  # A placeholder, not used dynamically here
    'catchup': False,
}

dag = DAG(
    'trigger_backfill_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

# Fetch backfill config from Airflow Variable
config = Variable.get("backfill_config", deserialize_json=True)
target_dag_id = config.get("target_dag_id", "target_dag")
start_date = datetime.strptime(config.get("start_date", "2024-02-20"), "%Y-%m-%d")
end_date = datetime.strptime(config.get("end_date", "2024-02-25"), "%Y-%m-%d")

# Generate backfill tasks dynamically
backfill_tasks = []
current_date = start_date
while current_date <= end_date:
    execution_date_str = current_date.strftime('%Y-%m-%d')

    trigger = TriggerDagRunOperator(
        task_id=f'trigger_{target_dag_id}_{execution_date_str}',
        trigger_dag_id=target_dag_id,
        conf={"execution_date": execution_date_str},  # Pass execution date as conf
        execution_date=current_date,  # Set execution date
        dag=dag,
    )
    
    backfill_tasks.append(trigger)
    current_date += timedelta(days=1)
