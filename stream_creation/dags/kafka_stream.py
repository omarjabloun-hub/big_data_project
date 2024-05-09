from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import csv
import uuid
import json
import time
from itertools import islice
from kafka import KafkaProducer

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 5, 9, 10, 00),
    'retries': 1,
}

def read_data(csv_reader):
    try:
        data = next(csv_reader)
        data['id'] = str(uuid.uuid4())
        return data
    except StopIteration:
        return None

def stream_data():
    import logging
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    curr_time = time.time()
    with open('../../dataset/customer_data.csv', 'r') as file:
        csv_reader = csv.DictReader(file)
        while True:
            time.sleep(1)
            if time.time() > curr_time + 60: #1 minute
                break
            try:
                res = read_data(csv_reader)
                if res is None:
                    break
                producer.send('customer_data_topic', value=res)
                print("Sent to Kafka : \n",res)
            except Exception as e:
                logging.error(f'An error occured: {e}')
                continue


with DAG('csv_to_kafka',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:


    kafka_task = PythonOperator(
        task_id='stream_data_to_kafka',
        python_callable=stream_data
    )

