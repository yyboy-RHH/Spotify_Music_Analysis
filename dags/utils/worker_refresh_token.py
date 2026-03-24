import requests
import base64
import time
from utils.GCP_client import get_bq_client
import os
import logging
from google.cloud import bigquery


def get_latest_token(current_worker: str) -> None:
    client = get_bq_client()
    with client:
        query = f"""
            SELECT *
            FROM affable-hydra-422306-r3.worker.{current_worker}
            ORDER BY access_last_update DESC LIMIT 1
        """
        query_job = client.query(query)
        rows = query_job.result()
        logging.info("Fetching latest token from BigQuery...")
        # access_token, access_last_update, refresh_token, refresh_last_update
        for row in rows:
            # print(type(row))
            # 返回第一行
            return row
        # 如果沒有行數據，返回 None 或者其他適當的值
        return None

# # get current workers' refresh token from GCP
# def get_latest_refresh_token(current_worker):
#     client = get_bq_client(CREDENTIAL_PATH)
#     with client:
#         query = f"""
#         SELECT *
#         FROM affable-hydra-422306-r3.worker.{current_worker}
#         ORDER BY refresh_last_update DESC LIMIT 1
#         """
#         query_job = client.query(query)
#         rows = query_job.result()
#         logging.info("Fetching latest refresh token from BigQuery...")
#         # access_token, access_last_update, refresh_token, refresh_last_update
#         for row in rows:
#             # 返回第一行
#             return row
#         # 如果沒有行數據，返回 None 或者其他適當的值
#         return None


def request_new_ac_token_refresh_token(current_worker: str,
                                       client_id: str,
                                       client_secret: str) -> str:
    '''
    request spotify token
    '''
    refresh_token = get_latest_token(current_worker)[2]
    credentials = f"{client_id}:{client_secret}"
    encoded_credentials = base64.b64encode(
        credentials.encode('utf-8')).decode('utf-8')

    data = {
        'client_id': client_id,
        'grant_type': 'refresh_token',
        'refresh_token': f'{refresh_token}'
    }
    headers = {'content-type': 'application/x-www-form-urlencoded',
               'Authorization': f"Basic {encoded_credentials}",
               }
    response = requests.post(
        'https://accounts.spotify.com/api/token', data=data, headers=headers, timeout=10)
    logging.info(response.text)
    access_token = response.json()['access_token']

    if 'refresh_token' not in response.json():
        refresh_token = refresh_token
    else:
        refresh_token = response.json()['refresh_token']

    client = get_bq_client()
    current_timestamp = int(time.time())
    try:
        query = f"""
            INSERT INTO affable-hydra-422306-r3.worker.{current_worker} (access_token, access_last_update, refresh_token, refresh_last_update)
            VALUES ('{access_token}', {current_timestamp},
                    '{refresh_token}', {current_timestamp})
        """
        client.query(query, location='EU')
        logging.info(f"Token successfully updated: {access_token}")
        return access_token

    except Exception as e:
        logging.info(f"Exception occur: {e}")


def get_workers() -> dict:

    workers = {
        "worker1": {"client_id": "d41d8cd98f00b204e9800998ecf8427e", "client_secret": "f96b697d7cb7938d525a2f31aaf161d0"},
        "worker2": {"client_id": "8b1a9953c4611296a827abf8c47804d7", "client_secret": "21232f297a57a5a743894a0e4a801fc3"},
        "worker3": {"client_id": "0cc175b9c0f1b6a831c399e269772661", "client_secret": "92eb5ffee6ae2fec3ad71c777531578f"},
        "worker4": {"client_id": "4a8a08f09d37b73795649038408b5f33", "client_secret": "8277e0910d750195b448797616e091ad"},
        "worker5": {"client_id": "e4da3b7fbbce2345d7772b0674a318d5", "client_secret": "115f89503138416a242f40fb7d7f338e"},
        "worker6": {"client_id": "336d5ebc5436534e61d16e63ddfca327", "client_secret": "1f82c235790d9b4b02554e2860b29c91"},
        "worker7": {"client_id": "502e4a16930e414107ee22b6198c578f", "client_secret": "a1d0c6e83f027327d8461063f4ac58a6"},
        "worker8": {"client_id": "7215ee9c7d9dc229d2921a40e899ec5f", "client_secret": "b8c37e33defde51cf91e1e03e51657da"},
        "worker9": {"client_id": "9a82cd93848b4887754b52b311743f54", "client_secret": "e10adc3949ba59abbe56e057f20f883e"},
        "worker10": {"client_id": "81dc9bdb52d04dc20036dbd8313ed055", "client_secret": "172522ec1028ab781d9dfd16eaca4cb6"}
    }
    return workers


def check_if_need_update_token(current_worker_name: str, current_worker: str) -> str:
    '''
    check if it update spotify token
    '''
    current_time = int(time.time())
    logging.info(f"Current worker: {current_worker_name}")

    client_id = current_worker["client_id"]
    secret = current_worker["client_secret"]
    access_last_update = get_latest_token(current_worker_name)[1]
    access_token = get_latest_token(current_worker_name)[0]

    logging.info(f"using access_token: {access_token}")

    # means token has been expired, default is 3600, make it 3500 to ensure we have enough time
    if access_last_update + 3500 <= current_time:
        # logging.info(f"access_last_update:{access_last_update} +3500 = {access_last_update+3500} current_time:{current_time}")
        access_token = request_new_ac_token_refresh_token(current_worker_name,
                                                          client_id, secret)

    return access_token
