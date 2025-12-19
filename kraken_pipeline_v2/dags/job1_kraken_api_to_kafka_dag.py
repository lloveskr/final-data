from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys


def run_producer():
    sys.path.append("/opt/airflow/src")
    from job1_producer import main
    main()


with DAG(
    dag_id="job1_kraken_api_to_kafka",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/1 * * * *",  # раз в минуту
    catchup=False,
    max_active_runs=1,
    tags=["kraken", "kafka", "ingestion"],
) as dag:

    PythonOperator(
      task_id="run_continuous_producer",
      python_callable=run_producer,
)

