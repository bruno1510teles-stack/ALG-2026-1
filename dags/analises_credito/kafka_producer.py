import pandas as pd
from confluent_kafka import Producer
from airflow.models import Variable

class Kafka:

    def send(topic, key, value):

        kafka_config = {
            'bootstrap.servers': Variable.get('KAFKA_DATALAKE_ENDPOINT')
        }

        producer = Producer(kafka_config)

        def delivery_report(err, msg):
            if err is not None:
                print(f"Erro ao enviar mensagem: {err}")
            else:
                print(f"Mensagem enviada para {msg.topic()} [{msg.partition()}]")
            
        producer.produce(topic= topic, key= key, value= value, callback= delivery_report)

        producer.flush()
