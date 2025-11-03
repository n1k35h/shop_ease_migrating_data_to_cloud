import awswrangler as wr
from aws_utils import aws_session

def transform_to_silver(**context):
    session = aws_session()

    ds = context["ds"]
    bucket = "shopease-db-datalake"
    glue_db = "shopease_db"

    tables = ["customers", "inventory", "products",
              "sale_transactions", "sales_managers", "stores"]

    for tbl in tables:
        path = f"s3://{bucket}/bronze/{tbl}/dt={ds}/*.csv"
        df = wr.s3.read_csv(
            path,
            boto3_session=session
            )

        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        if "order_date" in df.columns:
            partition_cols = ["order_date"]
        else:
            df["dt"] = ds
            partition_cols = ["dt"]

        wr.s3.to_parquet(
            df=df,
            path=f"s3://{bucket}/silver/{tbl}/",
            dataset=True,
            database=glue_db,
            table=f"{tbl}_silver",
            partition_cols=partition_cols,
            boto3_session=session
        )
    return "Data transformed to silver bucket"
