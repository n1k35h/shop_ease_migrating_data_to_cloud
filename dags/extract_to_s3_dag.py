from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from write_to_s3_silver import transform_to_silver
from write_to_s3_gold import s3_to_gold

BUCKET = "shopease-db-datalake"

default_args = {
    "owner": "airflow",
    "retries": 2,
    }

dag = DAG(
    dag_id="bronze_extract_shopease",
    description="Extracts tables from Postgres to s3 Bronze layer",
    start_date=datetime(2025, 10, 15),
    schedule_interval="0 4 * * *",
    catchup=False,
    default_args=default_args,
    tags=["bronze", "extract"]
)

tables = ["customers", "inventory", "products",
          "sale_transactions", "sales_managers", "stores"]

for tbl in tables:
    sql_to_s3_tables = SqlToS3Operator(
        task_id=f"extract_{tbl}_to_bronze",
        sql_conn_id="postgres_local",
        query=f"SELECT * FROM {tbl};",
        s3_bucket=BUCKET,
        s3_key=f"bronze/{tbl}/dt={{ ds }}/{tbl}.csv",
        aws_conn_id="aws_default",
        replace=True,
        file_format="csv",
        pd_kwargs={"index": False},
        dag=dag
    )

extract_to_silver = PythonOperator(
    task_id="extract_to_silver_s3",
    python_callable=transform_to_silver,
    dag=dag
)

# Dependencies: Bronze layer to Silver layer
sql_to_s3_tables >> extract_to_silver


extract_to_gold = PythonOperator(
    task_id="extract_to_gold_s3",
    python_callable=s3_to_gold,
    dag=dag
)
# Dependencies: Silver layer to Gold layer
extract_to_silver >> extract_to_gold
