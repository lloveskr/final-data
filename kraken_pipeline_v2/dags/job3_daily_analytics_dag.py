from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys


def run_daily_analytics():
    sys.path.append("/opt/airflow/src")
    from job3_daily_analytics import main
    main()


with DAG(
    dag_id="job3_daily_analytics",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["analytics", "sqlite", "daily"],
) as dag:

    PythonOperator(
        task_id="compute_daily_summary",
        python_callable=run_daily_analytics,
    )

