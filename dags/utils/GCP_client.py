from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
from google.cloud.exceptions import NotFound
import logging
import pandas as pd
import json
from typing import Union


def get_bq_client() -> bigquery.Client:
    """
    return BigQuery client
    """
    try:
        return bigquery.Client.from_service_account_json(
            "dags/cloud/affable-hydra-422306-r3-e77d83c42f33.json"
        )
    except Exception as e:
        logging.info(f"Error connecting to the BigQuery: {e}")
        return None


def get_storage_client() -> storage.Client:
    """
    return GCS client
    """
    try:
        return storage.Client.from_service_account_json(
            "dags/cloud/affable-hydra-422306-r3-e77d83c42f33.json"
        )
    except Exception as e:
        logging.info(f"Error connecting to the GCS: {e}")
        return None


def load_gcs_to_bigquery_external(
    gcs_uri: Union[str, list],
    dataset_id: str,
    table_id: str,
    external_source_format: str,
) -> None:
    """
    create a external table in GCS
    """
    bigquery_client = get_bq_client()

    query_job = bigquery_client.query(
        f"""
        CREATE OR REPLACE EXTERNAL TABLE `{dataset_id}.{table_id}`
        WITH PARTITION COLUMNS
        OPTIONS (
        format = '{external_source_format}',
        uris = ['{gcs_uri}']
        )
        """
    )

    logging.info(query_job.result())


def load_gcs_to_bigquery_native(
    gcs_uri: Union[str, list],
    dataset_id: str,
    table_id: str,
    schema: str,
    skip_rows: int,
) -> None:
    """
    create Native table in BigQuery
    """
    bigquery_client = get_bq_client()
    project_id = "affable-hydra-422306-r3"

    # 檢查DataSet & table是否存在，如果不存在，則創建
    try:
        dataset = bigquery_client.get_dataset(f"{project_id}.{dataset_id}")
    except Exception as e:
        logging.error(f"Dataset {dataset_id} does not exist. Creating dataset...")
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        dataset.location = "EU"
        dataset = bigquery_client.create_dataset(dataset)

    try:
        table = bigquery_client.get_table(f"{project_id}.{dataset_id}.{table_id}")
    except Exception as e:
        logging.error(
            f"Table {table_id} does not exist in dataset {dataset_id}. Creating table..."
        )
        table = bigquery.Table(f"{project_id}.{dataset_id}.{table_id}")
        table = bigquery_client.create_table(table)

    # bq 先定義config
    job_config = bigquery.LoadJobConfig(
        # schema=schema,
        skip_leading_rows=skip_rows,
        autodetect=True,
        # replace不是一直增加
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # bq load table from gcs uri
    job = bigquery_client.load_table_from_uri(
        gcs_uri,
        dataset_id + "." + table_id,
        job_config=job_config,
    )

    logging.info(f"{job.result()} - {dataset_id}.{table_id} Upload done!")


def save_progress_to_gcs(
    client: storage.Client, progress: pd.DataFrame, bucket_file_path: str
) -> None:
    """
    save progress to gcs
    """
    bucket = client.bucket("api_spotify_artists_tracks")
    blob = bucket.blob(bucket_file_path)
    blob.upload_from_string(json.dumps(progress), content_type="application/json")
    logging.info("save data to gcs!")
