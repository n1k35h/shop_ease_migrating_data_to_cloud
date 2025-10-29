# Migrating On-Premise Data to a cloud-based tool
Migrating On-Prem PostgreSQL Transactional Data to the Cloud with Airflow, S3, and Athena

This project demonstrates the migration of on-premise PostgreSQL transactional data to a cloud-based data lake using Apache Airflow, AWS S3, Glue, and Athena. The goal was to build an automated ETL pipeline following the Medallion Architecture (Bronze, Silver, Gold) to modernize data accessibility and enable analytics at scale.

### 🔧 Key Features
The Medallion Architecture:
-   🥉 Bronze Layer: Extract on-prem PostgreSQL data to AWS S3
-   🥈 Silver Layer: Transform CSV data into optimized Parquet files using Python and AWS Wrangler
-   🥇 Gold Layer: Load curated datasets into S3, cataloged with AWS Glue.

Query business-ready tables using AWS Athena for reporting (e.g., daily sales, top customers, category revenue).

Automated orchestration with Apache Airflow (Docker) for end-to-end workflow management.

### 🚀 Outcome

The solution enables fast, automated, and scalable data migration and reporting without relying on the on-prem database, reducing latency and improving analytical efficiency.
