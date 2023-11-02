from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kafka import KafkaProducer
import json
import time
import uuid
import logging
import requests

default_args = {
    'owner': 'Sanjay',
    'start_date': datetime(2023, 10, 24)
}

def get_data() -> json:
    
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    return res

def format_data(res) -> dict:
    
    data = {}
    #data['id'] = uuid.uuid4()
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['postcode'] = location['postcode']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['photo'] = res['picture']['medium']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']

    return data

def stream_data():
    
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=4000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            res = get_data()
            res = format_data(res)

            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

with DAG( 'user_automation',
         default_args= default_args,
         schedule = '@daily',
         catchup= False) as dag:

    streaming_task = PythonOperator(task_id = 'stream_data_from_api',
                                    python_callable= stream_data)
