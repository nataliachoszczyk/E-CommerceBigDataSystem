from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="users_batch_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Batch pipeline for users.csv"
) as dag:

    create_hdfs_users_dir = BashOperator(
        task_id="create_hdfs_users_dir",
        bash_command="hdfs dfs -mkdir -p /raw/users"
    )

    upload_users_csv = BashOperator(
        task_id="upload_users_csv",
        bash_command="""
        hdfs dfs -test -f /raw/users/users.csv || \
        hdfs dfs -put /home/win10/users.csv /raw/users/users.csv
        """
    )

    create_hdfs_users_dir >> upload_users_csv

    create_hive_users_table = BashOperator(
        task_id="create_hive_users_table",
        bash_command="hive -f /home/win10/sql/hive/tables/users_raw.hql"
    )

    upload_users_csv >> create_hive_users_table

    run_spark_users_processing = BashOperator(
        task_id="run_spark_users_processing",
        bash_command="spark-submit /home/win10/sql/spark/users_processing.py"
    )

    create_hive_users_table >> run_spark_users_processing
