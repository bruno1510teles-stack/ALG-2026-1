from datetime import datetime
import pandas as pd
from io import BytesIO
from minio import Minio
from confluent_kafka import Producer
import pytz

def envio_kafka_teste_camila(access_params):
    # Variáveis de Data
    fuso_horario = pytz.timezone('America/Sao_Paulo')
    agora = datetime.now(fuso_horario)
    ano = agora.strftime('%Y')
    mes = agora.strftime('%m')
    dia = agora.strftime('%d')
    hora = agora.strftime('%H')

    # Coletando pastas necessárias
    BUCKET_SOURCE_REFINED = "motor/"
    FOLDER_DESTINATION_REFINED = 'teste_camila/'

    # Conectando no MiniO
    client = Minio(
        access_params['endpoint_url_refined'],
        access_key=access_params['aws_access_key_id_refined'],
        secret_key=access_params['aws_secret_access_key_refined'],
    )

    # client = Minio(
    #     "api-refined.alpe.com.br",  # endpoint_url_raw
    #     access_key="FWaUneVAqRfB2oYrEmSK",      # access_key_id_raw
    #     secret_key="iaDAYImgToTykCYcR6cXwYCab2afSKmEaEhTY1gM"       # secret_key_raw
    # )

    # Listando todos os arquivos no diretório especificado
    objects = client.list_objects(BUCKET_SOURCE_REFINED, prefix=FOLDER_DESTINATION_REFINED, recursive=True)


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

 # Iterar sobre cada objeto listado e processar o envio para o Kafka
    for obj in objects:
        # Obtendo o arquivo CSV do MinIO
        file = client.get_object(
            bucket_name=BUCKET_SOURCE_REFINED, 
            object_name=obj.object_name
        )
        
        # Carregando o arquivo CSV em um DataFrame
        saida_politica = pd.read_csv(BytesIO(file.data), sep=';', dtype=str)
        
        # Iterar sobre as linhas do DataFrame e enviar para o Kafka
        for index, row in saida_politica.iterrows():
            key = row['cnpj_ec']
            value = row.to_json()
            producer.produce(topic, key=str(key), value=value, callback=delivery_report)

    # Esperar a entrega de todas as mensagens
    producer.flush()

if __name__ == "__main__":
    envio_kafka()