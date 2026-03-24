from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import pandas as pd
from utils.GCP_client import (
    get_bq_client,
    get_storage_client,
    load_gcs_to_bigquery_native,
    load_gcs_to_bigquery_external,
)
from google.cloud import bigquery


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 2),
    'email': ['famlitfriends@gmail.com'],  
    'email_on_failure': True,              # 失敗時，Airflow 自動發信
    'email_on_retry': False,               # 重試時要不要發信
}


def export_to_BQ():
    """
    export gcs data to BigQuery, and parameter define which type of table you want to export
    """

    table_type = "native"

    if table_type == "native":
        # Native table
        load_gcs_to_bigquery_native(
            gcs_uri=[
                "gs://api_spotify_artists_tracks/changeDataType/2017.csv",
                "gs://api_spotify_artists_tracks/changeDataType/2018.csv",
                "gs://api_spotify_artists_tracks/changeDataType/2019.csv",
                "gs://api_spotify_artists_tracks/changeDataType/2020.csv",
                "gs://api_spotify_artists_tracks/changeDataType/2021.csv",
                "gs://api_spotify_artists_tracks/changeDataType/2022.csv",
                "gs://api_spotify_artists_tracks/changeDataType/2023.csv",
                "gs://api_spotify_artists_tracks/changeDataType/2024.csv",
            ],
            dataset_id="stage_ChangeDataType",
            table_id=f"expand_table_2017_2024",
            schema=None,
            skip_rows=1,
        )
    elif table_type == "external":
        # external table
        load_gcs_to_bigquery_external(
            gcs_uri=[
                "gs://api_spotify_artists_tracks/changeDataType/2017.csv",
                "gs://api_spotify_artists_tracks/changeDataType/2018.csv",
                "gs://api_spotify_artists_tracks/changeDataType/2019.csv",
                "gs://api_spotify_artists_tracks/changeDataType/2020.csv",
                "gs://api_spotify_artists_tracks/changeDataType/2021.csv",
                "gs://api_spotify_artists_tracks/changeDataType/2022.csv",
                "gs://api_spotify_artists_tracks/changeDataType/2023.csv",
                "gs://api_spotify_artists_tracks/changeDataType/2024.csv",
            ],
            dataset_id="stage_ChangeDataType_ExternalTable",
            table_id=f"expand_table_2017_2024",
            external_source_format="CSV",
            schema=None,
        )


with DAG(
    "merge_chart_data_to_GCS.py",
    default_args=default_args,
    schedule_interval="@monthly",
    catchup=False,
) as dag:

    export_to_BQ = PythonOperator(
        task_id="export_to_BQ",
        python_callable=export_to_BQ,
        provide_context=True,
    )

    export_to_BQ
