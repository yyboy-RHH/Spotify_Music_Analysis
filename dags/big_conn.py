from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)

DEFAULT_DAG_ARGS = {
    "owner": "Airflow",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "start_date": datetime(2022, 1, 1),
}

with DAG(
    dag_id="create_simple_table",
    default_args=DEFAULT_DAG_ARGS,
    schedule_interval=None,
    catchup=False,
) as dag:
    create_dataset_task = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id="test_dataset",
        location="eu",
    )
    create_simple_table_task = BigQueryInsertJobOperator(
        task_id="create_simple_table",
        configuration={
            "query": {
                "query": "create_bq_table.sql",
                "useLegacySql": False,
            }
        },
    )

create_dataset_task >> create_simple_table_task
