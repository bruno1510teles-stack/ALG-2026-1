from datetime import datetime
import pandas as pd
from io import BytesIO
from minio import Minio
from confluent_kafka import Producer
import pytz
import time 

def envio_kafka(access_params, ti, intervalo=1):

    # Pega o df_resumido do XCom
    df_resumido_records = ti.xcom_pull(task_ids='executa_politica')
    
    # Converte de volta para DataFrame
    df_resumido = pd.DataFrame(df_resumido_records)
    df_resumido['cnpj_ec'] = df_resumido['cnpj_ec'].astype(str).str.zfill(14) 
    print(df_resumido)

    print(access_params['kafka_url'])

    # Configurações do Kafka
    kafka_config = {
        'bootstrap.servers': access_params['kafka_url']
    }
    topic = "credit-policy.v2.policy.resolution.out"

    # Criar o produtor Kafka
    producer = Producer(kafka_config)

    # Função de callback para verificar a entrega
    def delivery_report(err, msg):
        if err is not None:
            print(f"Erro ao enviar mensagem para CNPJ {msg.key()}: {err}")
        else:
            print(f"CNPJ {msg.key()} enviado com sucesso para {msg.topic()} [{msg.partition()}]")

        
    # Iterar sobre as linhas do DataFrame e enviar para o Kafka
    for index, row in df_resumido.iterrows():
        key = row['cnpj_ec']
        value = row.to_json()
        producer.produce(topic, key=str(key), value=value, callback=delivery_report)

    # Adiciona o intervalo entre as mensagens
        time.sleep(intervalo)

    # Esperar a entrega de todas as mensagens
    producer.flush()