import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from utils.GCP_client import (
    load_gcs_to_bigquery_native,
    load_gcs_to_bigquery_external,
)
from airflow.utils.dates import days_ago


BUCKET_FILE_PATH = {
    "getTrack": "gs://api_spotify_artists_tracks/output/worker_get_track_progress_1724.csv",
    "getTrackAudioAnalysis": "gs://api_spotify_artists_tracks/output/worker_get_track_audio_analysis_progress_1724.csv",
    "getTrackFeatures": "gs://api_spotify_artists_tracks/output/worker_get_track_audio_features_progress_1724.csv",
    "getArtist": "gs://api_spotify_artists_tracks/output/worker_get_artist_progress_1724.csv",
}
DATASET_ID_TRACK_EXTERNAL = "stage_TrackSet_external"
DATASET_ID_ARTIST_EXTERNAL = "stage_ArtistSet_external"
TABLE_TYPE_EXTERNAL = "external"
TABLE_TYPE_NATIVE = "native"
DATASET_ID_TRACK_NATIVE = "stage_TrackSet"
DATASET_ID_ARTIST_NATIVE = "stage_ArtistSet"


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
    table_type = TABLE_TYPE_NATIVE
    # schema = [
    #     bigquery.SchemaField("column1", "STRING"),
    #     bigquery.SchemaField("column2", "INTEGER"),
    # ]

    for api_name, gcs_uri in BUCKET_FILE_PATH.items():

        if table_type == "native":
            # Native table
            if api_name == "getArtist":
                load_gcs_to_bigquery_native(
                    gcs_uri=[gcs_uri],
                    dataset_id=DATASET_ID_ARTIST_NATIVE,
                    table_id=f"{api_name}",
                    schema=None,
                    skip_rows=1,
                )
            else:
                load_gcs_to_bigquery_native(
                    gcs_uri=[gcs_uri],
                    dataset_id=DATASET_ID_TRACK_NATIVE,
                    table_id=f"{api_name}",
                    schema=None,
                    skip_rows=1,
                )
        # external table
        elif table_type == "external":
            if api_name == "getArtist":
                load_gcs_to_bigquery_external(
                    gcs_uri=[gcs_uri],
                    dataset_id=DATASET_ID_ARTIST_EXTERNAL,
                    table_id=f"{api_name}",
                    external_source_format="CSV",
                    schema=None,
                )
            else:
                load_gcs_to_bigquery_external(
                    gcs_uri=[gcs_uri],
                    dataset_id=DATASET_ID_TRACK_EXTERNAL,
                    table_id=f"{api_name}",
                    external_source_format="CSV",
                    schema=None,
                )


from airflow.utils.db import provide_session
from airflow.models.dag import get_last_dagrun


def _get_execution_date_of(dag_id):
    @provide_session
    def _get_last_execution_date(exec_date, session=None, **kwargs):
        dag_a_last_run = get_last_dagrun(
            dag_id, session, include_externally_triggered=True
        )
        return dag_a_last_run.execution_date

    return _get_last_execution_date


with DAG(
    "workers_GCS_to_BQ_from_API.py",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    wait_for_workers_GetTrack = ExternalTaskSensor(
        task_id="wait_for_workers_GetTrack",
        external_dag_id="workers_GetTrack.py",
        external_task_id="process_data_in_gcs",
        mode="poke",
        execution_date_fn=_get_execution_date_of("workers_GetTrack.py"),
    )

    # Define sensors for other DAGs
    wait_for_workers_GetArtist = ExternalTaskSensor(
        task_id="wait_for_workers_GetArtist",
        external_dag_id="workers_GetArtist.py",
        external_task_id="process_data_in_gcs",
        mode="poke",
        execution_date_fn=_get_execution_date_of("workers_GetArtist.py"),
    )

    wait_for_workers_GetTrackAudioAnalysis = ExternalTaskSensor(
        task_id="wait_for_workers_GetTrackAudioAnalysis",
        external_dag_id="workers_GetTrackAudioAnalysis.py",
        external_task_id="process_data_in_gcs",
        mode="poke",
        execution_date_fn=_get_execution_date_of("workers_GetTrackAudioAnalysis.py"),
    )

    wait_for_workers_GetTrackAudioFeatures = ExternalTaskSensor(
        task_id="wait_for_workers_GetTrackAudioFeatures",
        external_dag_id="workers_GetTrackAudioFeatures.py",
        external_task_id="process_data_in_gcs",
        mode="poke",
        execution_date_fn=_get_execution_date_of("workers_GetTrackAudioFeatures.py"),
    )

    export_to_BQ = PythonOperator(
        task_id="export_to_BQ",
        python_callable=export_to_BQ,
        provide_context=True,
    )

    # Set task dependencies
    (
        [
            wait_for_workers_GetTrack,
            wait_for_workers_GetArtist,
            wait_for_workers_GetTrackAudioAnalysis,
            wait_for_workers_GetTrackAudioFeatures,
        ]
        >> export_to_BQ
    )

    # export_to_BQ
