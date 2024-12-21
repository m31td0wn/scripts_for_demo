import json
from typing import Dict, Optional

from confluent_kafka import Consumer, Producer
import redis
import psycopg

host = "rc1a-qfffabh2ud18i3sl.mdb.yandexcloud.net"
port = 9091
user = "producer_consumer"
password = "Megalord1993!"
topic = "order-service_orders"
cert_path = "C:\\Users\\79031\\.kafka\\YandexInternalRootCA.crt" #"D:\\yandex_practicum\\sprint9\\CA.pem"
group = "grp6"

def error_callback(err):
    print('Something went wrong: {}'.format(err))

params = {
            'bootstrap.servers': f'{host}:{port}',
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': cert_path,
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': user,
            'sasl.password': password,
            'group.id': group,  # '',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False,
            #'error_cb': error_callback,
            #'debug': 'all',
        }

redis_host = "c-c9q3ekmiov35o46re3gv.rw.mdb.yandexcloud.net"
redis_port = 6380

redis_client = redis.StrictRedis(
            host=redis_host,
            port=redis_port,
            password=password,
            ssl=True,
            ssl_ca_certs=cert_path)


consumer = Consumer(params)
consumer.subscribe([topic])

def consume(timeout: float = 3.0) -> Optional[Dict]:
    msg = consumer.poll(timeout=timeout)
    val = msg.value().decode()
    return json.loads(val)


def get(k) -> Dict:
        obj: str = redis_client.get(k)  # type: ignore
        return json.loads(obj)

while True:
    msg = consume()
    object_id = msg["object_id"]
    object_type = msg["object_type"]
    sent_dttm = msg["sent_dttm"]
    payload = json.dumps(msg['payload'])


    DB_HOST = "rc1b-z0490kgsd5v49ro7.mdb.yandexcloud.net"
    DB_PORT = 6432
    DB_NAME = "sprint9dwh"
    DB_USER = "db_user"

    try:
        # Установка соединения
        with psycopg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=password
        ) as conn:

            # Создание курсора и выполнение запроса
            with conn.cursor() as cur:
                cur.execute(
                    """
                        insert into stg.order_events(
                            object_id, 
                            object_type, 
                            sent_dttm, 
                            payload
                        )
                        values(
                            %(object_id)s, 
                            %(object_type)s,
                            %(sent_dttm)s, 
                            %(payload)s
                         )
                        ON CONFLICT (object_id) DO UPDATE set object_type = EXCLUDED.object_type, sent_dttm = EXCLUDED.sent_dttm, payload = EXCLUDED.payload;
                    """,
                    {
                        'object_id': object_id,
                        'object_type': object_type,
                        'sent_dttm': sent_dttm,
                        'payload': payload
                    }
                )

    except psycopg.Error as e:
        print(f"Ошибка подключения: {e}")

    print(msg)
