import logging
from utils.spotifyUri import (
    get_artist_uris,
    filter_artist_uris,
    check_missing_data,
    find_missing_data,
)
from utils.GCP_client import get_storage_client, save_progress_to_gcs
from utils.worker_refresh_token import get_workers, check_if_need_update_token
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
import itertools
import pandas as pd
from pandas import json_normalize
import requests
import json
import time
import random
from datetime import datetime
import urllib3
from requests.exceptions import SSLError, ConnectionError


urllib3.disable_warnings()

BUCKET_FILE_PATH = "process/worker_get_artist_progress_1724.json"
LOCAL_FILE_PATH = "worker_get_artist_progress_1724.csv"
API = "https://api.spotify.com/v1/artists/{}"
DATA_LIST_NAME = "artistData_list"
URI_TYPE = "artist"


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 2),
    'email': ['famlitfriends@gmail.com'],  
    'email_on_failure': True,              # 失敗時，Airflow 自動發信
    'email_on_retry': False,               # 重試時要不要發信
}

def for_loop_get_response(artist_uris: list, artistData_list: list) -> list:
    """
    for loop to get API response
    """
    start_time = int(time.time())

    workers = get_workers()
    worker_cycle = itertools.cycle(workers.items())
    current_worker_name, current_worker = next(worker_cycle)

    # 是下面的for迴圈 count % 100
    count = 1

    for artist_uri in artist_uris:

        current_time = int(time.time())
        elapsed_time = current_time - start_time

        if elapsed_time >= 30:
            start_time = current_time
            print(f"{elapsed_time} - Doing switch worker !!")
            current_worker_name, current_worker = next(worker_cycle)
            time.sleep(1)

        access_token = check_if_need_update_token(current_worker_name, current_worker)

        headers = {
            "accept": "*/*",
            "accept-language": "zh-TW,zh;q=0.8",
            "authorization": f"Bearer {access_token}",
            "origin": "https://developer.spotify.com",
            "referer": "https://developer.spotify.com/",
            "sec-ch-ua": '"Brave";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "sec-gpc": "1",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Connection": "close",
        }

        get_artist_url = API.format(artist_uri)
        print(get_artist_url)

        try:
            response = requests.get(get_artist_url, headers=headers, verify=False)

            if response.status_code == 429:
                logging.info(f"Reach the request limitation, change the worker now!")
                time.sleep(10)
                access_token = check_if_need_update_token(
                    current_worker_name, current_worker
                )
                response = requests.get(
                    get_artist_url,
                    headers={
                        "accept": "*/*",
                        "accept-language": "zh-TW,zh;q=0.8",
                        "authorization": f"Bearer {access_token}",
                        "origin": "https://developer.spotify.com",
                        "referer": "https://developer.spotify.com/",
                        "sec-ch-ua": '"Brave";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
                        "sec-ch-ua-mobile": "?0",
                        "sec-ch-ua-platform": '"macOS"',
                        "sec-fetch-dest": "empty",
                        "sec-fetch-mode": "cors",
                        "sec-fetch-site": "same-site",
                        "sec-gpc": "1",
                        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                        "Connection": "close",
                    },
                    verify=False,
                )

            artist_data = response.json()
            artistData_list.append(artist_data)

            count += 1
            logging.info(f"{count}-{artist_uri}")

            # n = random.randint(1,3)  ## gen 1~3s
            time.sleep(1)

            # 每100筆睡2秒
            if count % 100 == 0:
                time.sleep(2)
                client = get_storage_client()

                progress = {
                    "last_artist_uri": artist_uri,
                    DATA_LIST_NAME: artistData_list,
                }

                # save progress to GCS
                save_progress_to_gcs(client, progress, BUCKET_FILE_PATH)

        except (SSLError, ConnectionError) as e:
            response = requests.get(get_artist_url, headers=headers, verify=False)
            logging.info(f"get the {e} data again done!")

            artist_data = response.json()
            artistData_list.append(artist_data)

            count += 1
            logging.info(f"{count}-{artist_uri}")

            client = get_storage_client()

            progress = {
                "last_artist_uri": artist_uri,
                DATA_LIST_NAME: artistData_list,
            }

            # save progress to GCS
            save_progress_to_gcs(client, progress, BUCKET_FILE_PATH)

            raise AirflowFailException("Connection error, marking DAG as failed.")

    return artistData_list


def get_artist_data(**context):
    """
    fetch Spotify Developer API - get artist, this function will push response list result to next dag

    """

    df = get_artist_uris()
    artist_uris = list(df["artistUri"].drop_duplicates())  # distinct artistUri

    # read form gcs
    # try reload progress from GCS
    client = get_storage_client()

    bucket = client.bucket("api_spotify_artists_tracks")
    blob = bucket.blob(BUCKET_FILE_PATH)

    if blob.exists():
        progress = json.loads(blob.download_as_text())
        if isinstance(progress, dict):
            artistData_list = progress[DATA_LIST_NAME]
            last_artist_uri = progress["last_artist_uri"]
            artist_uris = filter_artist_uris(artist_uris, last_artist_uri)
            artistData_list = for_loop_get_response(artist_uris, artistData_list)
        else:
            artistData_list = progress

    else:
        artistData_list = []
        artistData_list = for_loop_get_response(artist_uris, artistData_list)

    # save progress to GCS
    save_progress_to_gcs(client, artistData_list, BUCKET_FILE_PATH)
    context["ti"].xcom_push(key="result", value=artistData_list)


# 確保沒有缺失值
def check_no_missing_data(**context):
    """
    make sure no missing data from API
    """
    Data_list = context["ti"].xcom_pull(task_ids="get_artist_data", key="result")

    # 去除重複
    # 用於保存不重複的字典
    artistData_set = set()

    # 遍歷字典列表，將字典轉換為 JSON 字符串並添加到集合中
    for d in Data_list:
        # 將字典轉換為 JSON 字符串並添加到集合中
        artistData_set.add(json.dumps(d, sort_keys=True))

    # 將集合中的 JSON 字符串轉換回字典
    artistData_list = [json.loads(s) for s in artistData_set]

    if check_missing_data(URI_TYPE, data=artistData_list):
        client = get_storage_client()
        save_progress_to_gcs(client, artistData_list, BUCKET_FILE_PATH)
        logging.info(
            f"If you see this, means you get the whole data - {len(artistData_list)} from get_artist API!"
        )
    else:
        # get API data again and put missing data in artistData_list
        artist_uris = find_missing_data(URI_TYPE, data=artistData_list)
        artistData_list = for_loop_get_response(
            artist_uris, artistData_list=artistData_list
        )
        logging.info(f"Get the missing data done! There is {len(artistData_list)} data")
        client = get_storage_client()
        save_progress_to_gcs(client, artistData_list, BUCKET_FILE_PATH)


def process_data_in_gcs():

    client = get_storage_client()
    bucket = client.bucket("api_spotify_artists_tracks")
    blob = bucket.blob(BUCKET_FILE_PATH)

    artistData_list = json.loads(blob.download_as_text())

    # Extend data
    df = pd.json_normalize(artistData_list).drop(columns=["images"])
    df_genres = df.explode("genres")
    df_final = df_genres.rename(columns=lambda x: x.replace(".", "_")).drop_duplicates()

    # Upload to GCS
    local_file_path = LOCAL_FILE_PATH
    gcs_bucket = "api_spotify_artists_tracks"
    gcs_file_name = f"output/{local_file_path}"

    df_final.to_csv(local_file_path, index=False)

    bucket = client.get_bucket(gcs_bucket)
    blob = bucket.blob(gcs_file_name)

    blob.upload_from_filename(local_file_path)


with DAG(
    "workers_GetArtist.py",  # <--- dag_id
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    get_artist_data = PythonOperator(
        task_id="get_artist_data",
        python_callable=get_artist_data,
        provide_context=True,
    )

    check_no_missing_data = PythonOperator(
        task_id="check_no_missing_data",
        python_callable=check_no_missing_data,
        provide_context=True,
    )

    process_data_in_gcs = PythonOperator(
        task_id="process_data_in_gcs",
        python_callable=process_data_in_gcs,
        provide_context=True,
    )

# Order of DAGs
get_artist_data >> check_no_missing_data >> process_data_in_gcs
