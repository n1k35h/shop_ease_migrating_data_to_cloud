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

# Set dependencies: all bronze extraction tasks must finish before silver transformation
sql_to_s3_tables >> extract_to_silver


extract_to_gold = PythonOperator(
    task_id="extract_to_gold_s3",
    python_callable=s3_to_gold,
    dag=dag
)
extract_to_silver >> extract_to_gold



# Customers
# sql_to_s3_customers = SqlToS3Operator(
#     task_id = "extract_customers_to_bronze",
#     sql_conn_id = "postgres_local",
#     query = "SELECT * FROM customers;",
#     s3_bucket = BUCKET,
#     s3_key = "shopease-db-datalake/bronze/customers/dt={{ ds }}/customers.csv",
#     aws_conn_id="aws_default",
#     replace = True,
#     file_format = "csv",
#     pd_kwargs= {"index": False},
#     dag = dag
#     )

# # Inventory
# sql_to_s3_inventory = SqlToS3Operator(
#     task_id = "extract_inventory_to_bronze",
#     sql_conn_id = "postgres_local",
#     query = "SELECT * FROM inventory;",
#     s3_bucket = BUCKET,
#     s3_key = "shopease-db-datalake/bronze/inventory/dt={{ ds }}/inventory.csv",
#     aws_conn_id="aws_default",
#     replace = True,
#     file_format = "csv",
#     pd_kwargs= {"index": False},
#     dag = dag
#     )

# # Products
# sql_to_s3_products = SqlToS3Operator(
#     task_id = "extract_products_to_bronze",
#     sql_conn_id = "postgres_local",
#     query = "SELECT * FROM products;",
#     s3_bucket = BUCKET,
#     s3_key = "shopease-db-datalake/bronze/products/dt={{ ds }}/products.csv",
#     aws_conn_id="aws_default",
#     replace = True,
#     file_format = "csv",
#     pd_kwargs= {"index": False},
#     dag = dag
#     )

# # Sale Transactions
# sql_to_s3_sale_transactions = SqlToS3Operator(
#     task_id = "extract_sale_transactions_to_bronze",
#     sql_conn_id = "postgres_local",
#     query = "SELECT * FROM sale_transactions;",
#     s3_bucket = BUCKET,
#     s3_key = "shopease-db-datalake/bronze/sale_transactionss/dt={{ ds }}/sale_transactions.csv",
#     aws_conn_id="aws_default",
#     replace = True,
#     file_format = "csv",
#     pd_kwargs= {"index": False},
#     dag = dag
#     )

# # Sales Managers
# sql_to_s3_sales_managers = SqlToS3Operator(
#     task_id = "extract_sales_managers_to_bronze",
#     sql_conn_id = "postgres_local",
#     query = "SELECT * FROM sales_managers;",
#     s3_bucket = BUCKET,
#     s3_key = "shopease-db-datalake/bronze/sales_managers/dt={{ ds }}/sales_managers.csv",
#     aws_conn_id="aws_default",
#     replace = True,
#     file_format = "csv",
#     pd_kwargs= {"index": False},
#     dag = dag
#     )

# # Stores
# sql_to_s3_stores = SqlToS3Operator(
#     task_id = "extract_stores_to_bronze",
#     sql_conn_id = "postgres_local",
#     query = "SELECT * FROM stores;",
#     s3_bucket = BUCKET,
#     s3_key = "shopease-db-datalake/bronze/stores/dt={{ ds }}/stores.csv",
#     aws_conn_id="aws_default",
#     replace = True,
#     file_format = "csv",
#     pd_kwargs= {"index": False},
#     dag = dag
#     )


