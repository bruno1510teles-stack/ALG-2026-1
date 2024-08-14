from datetime import datetime
import pandas as pd
from io import BytesIO
from minio import Minio
from confluent_kafka import Producer

def envio_kafka(access_params):
    # Variáveis de Data
    agora = datetime.now()
    ano = agora.strftime('%Y')
    mes = agora.strftime('%m')
    dia = agora.strftime('%d')

    # Coletando pastas necessárias
    BUCKET_SOURCE_REFINED = "motor"
    FOLDER_DESTINATION_REFINED = 'analise_credito/out'

    # Conectando no MiniO
    client = Minio(
        access_params['endpoint_url_refined'],
        access_key=access_params['aws_access_key_id_refined'],
        secret_key=access_params['aws_secret_access_key_refined'],
    )

    # Obtendo o arquivo CSV do MinIO
    file = client.get_object(
        bucket_name=BUCKET_SOURCE_REFINED, 
        object_name=f'{FOLDER_DESTINATION_REFINED}/{ano}/{mes}/{dia}/RESPOSTA_MOTOR_RESUMIDA.csv'
    )

    saida_politica = pd.read_csv(BytesIO(file.data), sep=';', dtype=str)

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
            print(f"Erro ao enviar mensagem: {err}")
        else:
            print(f"Mensagem enviada para {msg.topic()} [{msg.partition()}]")

    # Iterar sobre as linhas do DataFrame e enviar para o Kafka
    for index, row in saida_politica.iterrows():
        key = row['cnpj_ec']
        value = row.to_json()
        producer.produce(topic, key=str(key), value=value, callback=delivery_report)
    
    # Esperar a entrega de todas as mensagens
    producer.flush()

if __name__ == "__main__":
    envio_kafka()