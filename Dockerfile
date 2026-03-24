FROM apache/airflow:2.8.4
COPY requirements.txt .
COPY config/airflow.cfg /opt/airflow/airflow.cfg
RUN pip install -r requirements.txt
