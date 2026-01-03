from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="products_batch_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Batch pipeline for products.csv"
) as dag:

    create_hdfs_products_dir = BashOperator(
        task_id="create_hdfs_products_dir",
        bash_command="hdfs dfs -mkdir -p /raw/products"
    )

    upload_products_csv = BashOperator(
        task_id="upload_products_csv",
        bash_command="""
        hdfs dfs -test -f /raw/products/products.csv || \
        hdfs dfs -put /home/win10/products.csv /raw/products/products.csv
        """
    )

    create_hdfs_products_dir >> upload_products_csv

    create_hive_products_table = BashOperator(
        task_id="create_hive_products_table",
        bash_command="hive -f /home/win10/sql/hive/tables/products_raw.hql"
    )

    upload_products_csv >> create_hive_products_table

    run_spark_products_processing = BashOperator(
        task_id="run_spark_products_processing",
        bash_command="spark-submit /home/win10/sql/spark/products_processing.py"
    )

    create_hive_products_table >> run_spark_products_processing
