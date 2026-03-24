from google.cloud import bigquery
from google.oauth2 import service_account
import json
import logging
import requests
import base64
import time
from datetime import datetime

'''
This is used for GCP Cloud 
The Tokens will be saved to BigQuery/ airflow.tokens
And this will be uploaded to GCP
'''


def create_bigquery_client():
    try:
        credentials = service_account.Credentials.from_service_account_file('/opt/airflow/cloud/affable-hydra-422306-r3-48540d47aef8.json') 
        client = bigquery.Client(credentials=credentials)
        return client
    
    except Exception as e:
        print(f"Error connecting to Google Cloud BigQuery: {e}")
        return None

def get_latest_ac_token_gcp():
    client = create_bigquery_client()
    with client:
        query_job = client.query(
            "SELECT * FROM `affable-hydra-422306-r3.airflow.tokens` ORDER BY access_last_update DESC LIMIT 1")
        rows = query_job.result()

        logging.info("Fetching latest access token from BigQuery...")
        for row in rows:
            row_dict = dict(row)
        return row_dict

def get_latest_refresh_token_gcp():
    client = create_bigquery_client()
    with client:
        query_job = client.query(
            "SELECT * FROM `affable-hydra-422306-r3.airflow.tokens` ORDER BY refresh_last_update DESC LIMIT 1")
        rows = query_job.result()

        logging.info("Fetching latest refresh token from BigQuery...")
        for row in rows:
            row_dict = dict(row)
        return row_dict["refresh_token"]


def request_new_ac_token_refresh_token_gcp():

    refresh_token = get_latest_refresh_token_gcp()
    client_id = '3140d7f560664be9a52544791c13b670'
    client_secret = 'cfb94502d418483a9fa631e4d7d23691'
    credentials = f"{client_id}:{client_secret}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

    data = {
        'client_id': '3140d7f560664be9a52544791c13b670',
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token
    }
    headers = {'content-type': 'application/x-www-form-urlencoded',  #It means { key ,value } format
               'Authorization': f"Basic {encoded_credentials}",
               }
    response = requests.post('https://accounts.spotify.com/api/token', data=data, headers=headers,timeout=10)
    access_token = response.json()['access_token']
    if 'refresh_token' not in response.json():
        refresh_token = refresh_token
    else:
        refresh_token = response.json()['refresh_token']

    client = create_bigquery_client()
    current_timestamp = int(time.time())
    with client:
        try:
            query_job = client.query(
                f"INSERT INTO airflow.tokens (access_token, access_last_update, refresh_token, refresh_last_update) \
                VALUES ('{access_token}', {current_timestamp}, '{refresh_token}', {current_timestamp})"
            )
            logging.info(f"Token successfully updated: {access_token}")
            return access_token
        except Exception as e:
            logging.error(f"Exception occur: {e}")


