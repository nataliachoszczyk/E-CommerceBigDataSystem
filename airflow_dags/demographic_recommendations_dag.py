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
    'demographic_recommendations_daily',
    default_args=default_args,
    description='Daily Demographic Recommendations (24h data)',
    schedule_interval='17 4 * * *',  # 4:17 AM codziennie
    start_date=datetime(2026, 1, 2),
    catchup=False,
    tags=['analytics', 'recommendations', 'daily'],
) as dag:

    run_demographics = BashOperator(
        task_id='run_demographic_recommendations',
        bash_command='spark-submit /home/win10/analytics_scripts/demographic_recommendations.py',
        execution_timeout=timedelta(minutes=30),
    )
