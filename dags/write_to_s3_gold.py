import awswrangler as wr
from aws_utils import aws_session

session = aws_session()

query = """
SELECT store_id, COUNT(*) AS total_sales,
SUM(quantity * unit_price) AS total_revenue
FROM sale_transactions_silver
GROUP BY store_id;
"""

def s3_to_gold():
    df = wr.athena.read_sql_query(
        query,
        database="shopease_db",
        boto3_session=session
    )

    wr.s3.to_parquet(
        df=df,
        path="s3://shopease-db-datalake/gold/sales_summary/",
        dataset=True,
        database="shopease_db",
        table="sales_summary_gold",
        boto3_session=session
    )
    