FROM apache/airflow:2.9.2
WORKDIR /airflow
COPY requirements.txt /airflow
RUN pip install -r requirements.txt
