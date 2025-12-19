from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys


def run_consumer_cleaner():
    sys.path.append("/opt/airflow/src")
    from job2_consumer_cleaner import main
    main()


with DAG(
    dag_id="job2_kafka_to_sqlite",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["kafka", "sqlite", "cleaning"],
) as dag:

    PythonOperator(
        task_id="consume_clean_write_sqlite",
        python_callable=run_consumer_cleaner,
    )

