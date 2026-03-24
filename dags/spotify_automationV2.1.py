import random
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
from kafka3 import KafkaProducer, KafkaConsumer
import requests
import logging
import psycopg2
import sys
from airflow.models import Variable
import time
from refresh_token.refresh_token_gcp import create_bigquery_client
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from refresh_token.refresh_token_gcp import get_latest_ac_token_gcp, request_new_ac_token_refresh_token_gcp

## Setting Enviroment Variable ##
Variable.set("broker", "broker:29092")
Variable.set("group_id", "test-consumer-group")

## Getting Enviroment Variable ##
broker = Variable.get("broker")
group_id = Variable.get("group_id")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 2),
    'email': ['famlitfriends@gmail.com'],  
    'email_on_failure': True,              # 失敗時，Airflow 自動發信
    'email_on_retry': False,               # 重試時要不要發信
}

def check_if_need_update_token(**context):
    current_timestamp = int(time.time())
    ac_dict = get_latest_ac_token_gcp()
    access_token = ac_dict["access_token"]
    last_ac_time = ac_dict["access_last_update"]

    if last_ac_time + 3000 < current_timestamp: # means token has been expired, default is 3600, make it 3000 to ensure we have enough time
        access_token = request_new_ac_token_refresh_token_gcp()
    context['ti'].xcom_push(key='access_token', value=access_token)

def get_data(**context):

        access_token = context['task_instance'].xcom_pull(task_ids='check_if_need_update_token', key='access_token')
        producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
        target_years = [str(year) for year in range(2017, 2024)]
        normal_year = {"01": 31, "02": 28, "03": 31, "04": 30, "05": 31, "06": 30, 
                       "07": 31, "08": 31, "09": 30, "10": 31, "11": 30, "12": 31}
        special_year = {"01": 31, "02": 29, "03": 31, "04": 30, "05": 31, "06": 30, 
                        "07": 31, "08": 31, "09": 30, "10": 31, "11": 30, "12": 31}
        # scope_of_2024 = {"01": 31, "02": 29, "03": 31}

        topics = []
        for char in target_years:
            if char in ["2020", "2024"]:  #2020, 2024 are special years
                decided_year = special_year
            else:
                decided_year = normal_year

            for month, last_date_of_month in decided_year.items():
                year = char
                days, end = 1, last_date_of_month
                while days <= end:
                    headers = {
                        'authority': 'charts-spotify-com-service.spotify.com',
                        'accept': 'application/json',
                        'accept-language': 'zh-TW,zh;q=0.9',
                        'app-platform': 'Browser',
                        'authorization': f'Bearer {access_token}',  
                        'content-type': 'application/json',
                        'origin': 'https://charts.spotify.com',
                        'referer': 'https://charts.spotify.com/',
                        'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Brave";v="122"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"macOS"',
                        'sec-fetch-dest': 'empty',
                        'sec-fetch-mode': 'cors',
                        'sec-fetch-site': 'same-site',
                        'sec-gpc': '1',
                        'spotify-app-version': '0.0.0.production',
                        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                    }

                    logging.info(f"Using access_token:{access_token}")
                    url = f'https://charts-spotify-com-service.spotify.com/auth/v0/charts/regional-global-daily/{year}-{month}-{str(days).zfill(2)}'  # 要呼叫的 API endpoint
                    logging.info(f"Using url: {url}")
                    response = requests.get(
                        url,
                        headers=headers,
                    )
                    while response.status_code == 429:
                        logging.info(f"Reach the request limitation, doing time sleep now!")
                        time.sleep(30)
                        response = requests.get(
                            url,
                            headers=headers,
                        )
                    if response.status_code != 200 and response.status_code != 429:# token expired
                        logging.info(f"Request a new token for retry")
                        access_token = request_new_ac_token_refresh_token_gcp()
                        response = requests.get(
                            url,
                            headers = {
                        'authority': 'charts-spotify-com-service.spotify.com',
                        'accept': 'application/json',
                        'accept-language': 'zh-TW,zh;q=0.9',
                        'app-platform': 'Browser',
                        'authorization': f'Bearer {access_token}',
                        'content-type': 'application/json',
                        'origin': 'https://charts.spotify.com',
                        'referer': 'https://charts.spotify.com/',
                        'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Brave";v="122"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"macOS"',
                        'sec-fetch-dest': 'empty',
                        'sec-fetch-mode': 'cors',
                        'sec-fetch-site': 'same-site',
                        'sec-gpc': '1',
                        'spotify-app-version': '0.0.0.production',
                        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                    },
                        )
                    entries = response.json()["entries"]  # / Q: 解析 API 回傳的 JSON = return response.json() /
                    chart_date = f"{month}-{str(days).zfill(2)}"
                    producer.send(f"{year}{month}", key=chart_date.encode('utf-8'),  value=json.dumps(entries).encode('utf-8'))
                    logging.info(f'data sent: {entries}')
                    days += 1

                    n = random.randint(1,3) ## gen 1~3s
                    time.sleep(n)
                topics.append(f"{year}{month}")
            topics_json = json.dumps(topics)
            context['ti'].xcom_push(key='topics', value=topics_json)

def consume_msg_from_kafka(**context):
    topics_json = context['task_instance'].xcom_pull(task_ids='get_data', key='topics')
    topics = json.loads(topics_json)

    for topic in topics:
        year = topic[:4]
        logging.info(f"Handling topics{topic}")
        consumer = KafkaConsumer(topic,
                                 group_id=group_id,
                                 bootstrap_servers=broker,
                                 auto_offset_reset='earliest',
                                 enable_auto_commit=True,
                                 max_poll_interval_ms=300000,
                                 max_poll_records=1,
                                 consumer_timeout_ms=1000
                                 )
        ## create a db connection
        client = create_bigquery_client()
        try:
            for message in consumer:
                if message is not None:
                    chart_date = message.key.decode('utf-8')
                    logging.info(f"Key char_date: {chart_date}")
                    logging.info(f"Received message: {message.value}")
                    consumed_msg = json.loads(message.value)
                    consume_and_store_data(chart_date,consumed_msg, client, year)

                else:
                    print("Received None message. Exiting loop.")
                    break
        except Exception as e:
            print(f"Errors! please check {e}")
        finally:
            consumer.close()
            if client:
                client.close()

def consume_and_store_data(chart_date, kafka_msg, client, year):
    try:
        chart_date = chart_date
        with client as bq_client:
            current_timestamp = int(time.time())
            table_name = f"raw_data_{year}0101_{year}1231"
            table_ref = bq_client.dataset("airflow").table(table_name)
            schema = [
                bigquery.SchemaField("chart_date", "STRING"),
                bigquery.SchemaField("chartEntryData", "STRING"),
                bigquery.SchemaField("missingRequiredFields", "BOOL"),
                bigquery.SchemaField("trackMetadata", "STRING"),
                bigquery.SchemaField("update_time", "INTEGER")
            ]

            try:
                table = bq_client.get_table(table_ref)
            except NotFound:

                table = bigquery.Table(table_ref, schema=schema)
                bq_client.create_table(table)

            rows_to_insert = []
            for data in range(0, len(kafka_msg)):
                chartEntryData = kafka_msg[data]['chartEntryData']
                missingRequiredFields = kafka_msg[data]['missingRequiredFields']
                trackMetadata = kafka_msg[data]['trackMetadata']
                rows_to_insert.append((chart_date, json.dumps(chartEntryData), missingRequiredFields,
                                       json.dumps(trackMetadata), current_timestamp))

            errors = bq_client.insert_rows(table_ref, rows_to_insert, selected_fields=schema)
            if errors:
                print(f"Errors while inserting rows: {errors}")

            print("job>>>>", errors)
            print("len of job>>>>", len(rows_to_insert))
    except Exception as e:
        print(f"Error storing data: {e}")


with DAG('spotify_automation_V2.1.py',
         default_args=default_args,
         schedule_interval="@monthly",
         catchup=False) as dag:
    check_if_need_update_token = PythonOperator(
        task_id='check_if_need_update_token',
        python_callable=check_if_need_update_token
    )
    get_data = PythonOperator(
        task_id='get_data',
        python_callable=get_data,
        provide_context=True,
    )

    consume_msg_from_kafka = PythonOperator(
        task_id='consume_msg_from_kafka',
        python_callable=consume_msg_from_kafka,
        provide_context=True,
    )

check_if_need_update_token >> get_data >> consume_msg_from_kafka
