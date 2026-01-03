from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'win10',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'brand_performance_daily',
    default_args=default_args,
    description='Daily Brand Performance Analysis (7 days data)',
    schedule_interval='17 5 * * *',  # 5:17 AM codziennie
    start_date=datetime(2026, 1, 2),
    catchup=False,
    tags=['analytics', 'brand', 'daily'],
) as dag:

    run_brand_performance = BashOperator(
        task_id='run_brand_performance_analysis',
        bash_command='spark-submit /home/win10/analytics_scripts/brand_performance_analysis.py',
        execution_timeout=timedelta(minutes=45),
    )
