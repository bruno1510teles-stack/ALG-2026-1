# Importing Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake
import unicodedata
import re

def get_bases_licitacoes (access_params=None):

    minio_trusted = Minio(
        access_params['endpoint_url_trusted'],
        access_key=access_params['aws_access_key_id_trusted'],
        secret_key=access_params['aws_secret_access_key_trusted'],
    )

    # Connection validation
    try:
        # Try to list the buckets
        
        buckets = minio_trusted.list_buckets()
        
        # If the connection was successful, print the buckests
        print("Conexão bem-sucedida. Lista de buckets disponíveis:")
        for bucket in buckets:
            print(bucket.name)
        
    except Exception as e:
            # If the connection was failed, print the error message
            print(f"Erro ao conectar ao MinIO: {e}")

    
    BUCKET_SOURCE_RAW = "receita-federal"
    FOLDER_DESTINATION_RAW = 'licitacoes'


    print('Carregando dados Item Licitação')

    # Compras
    file_item_licitacao = '202404_ItemLicitação.csv'
    path_item_licitacao = f'{FOLDER_DESTINATION_RAW}/{file_item_licitacao}'

    response_item_licitacao = minio_trusted.get_object(BUCKET_SOURCE_RAW, path_item_licitacao)
    file_data_item_licitacao = BytesIO(response_item_licitacao.read())
    item_licitacao = pd.read_csv(file_data_item_licitacao, sep=';', quotechar='"', encoding='latin1')

    print('Tamanho da base Item Licitação:')
    print(len(item_licitacao))



    print('Carregando dados Licitação')

    # Compras
    file_licitacao = '202404_Licitação.csv'
    path_licitacao = f'{FOLDER_DESTINATION_RAW}/{file_licitacao}'

    response_licitacao = minio_trusted.get_object(BUCKET_SOURCE_RAW, path_licitacao)
    file_data_licitacao = BytesIO(response_licitacao.read())
    licitacao = pd.read_csv(file_data_licitacao, sep=';', quotechar='"', encoding='latin1')

    print('Tamanho da base Licitação:')
    print(len(licitacao))



    print('Carregando dados Participantes Licitação')

    # Compras
    file_participantes_licitacao = '202404_ParticipantesLicitação.csv'
    path_participantes_licitacao = f'{FOLDER_DESTINATION_RAW}/{file_participantes_licitacao}'

    response_participantes_licitacao = minio_trusted.get_object(BUCKET_SOURCE_RAW, path_participantes_licitacao)
    file_data_participantes_licitacao = BytesIO(response_participantes_licitacao.read())
    participantes_licitacao = pd.read_csv(file_data_participantes_licitacao, sep=';', quotechar='"', encoding='latin1')

    print('Tamanho da base Participantes Licitação:')
    print(len(participantes_licitacao))



    def normalizar_colunas(col):
        col = unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('utf-8')

        col = col.replace(' ', '_')
        
        col = re.sub(r'[^\w_]', '', col)

        return col.lower()
    

    # Tratando nome das colunas de todos os Dataframes

    item_licitacao.columns = [normalizar_colunas(col) for col in item_licitacao.columns]

    licitacao.columns = [normalizar_colunas(col) for col in licitacao.columns]

    participantes_licitacao.columns = [normalizar_colunas(col) for col in participantes_licitacao.columns]



    now = datetime.now(tz=timezone(timedelta(hours=-3)))

    item_licitacao['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    item_licitacao['year'], item_licitacao['month'], item_licitacao['day'] = now.year, now.month, now.day
    item_licitacao = item_licitacao.reset_index(drop=True)

    licitacao['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    licitacao['year'], licitacao['month'], licitacao['day'] = now.year, now.month, now.day
    licitacao = licitacao.reset_index(drop=True)

    participantes_licitacao['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    participantes_licitacao['year'], participantes_licitacao['month'], participantes_licitacao['day'] = now.year, now.month, now.day
    participantes_licitacao = participantes_licitacao.reset_index(drop=True)

    # Exportando 

    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }


    print('Exportando dados Item Licitacao...')

    BUCKET_COMPRAS = "receita-federal/licitacoes/item_licitacao/delta"
    write_deltalake(
        f"s3a://{BUCKET_COMPRAS}", 
        item_licitacao, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )


    print('Exportando dados Licitação...')

    BUCKET_ITEM_COMPRA = "receita-federal/licitacoes/licitacao/delta"
    write_deltalake(
        f"s3a://{BUCKET_ITEM_COMPRA}", 
        licitacao, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )


    print('Exportando dados Participantes Licitação...')

    BUCKET_TERMO_ADITIVO = "receita-federal/licitacoes/participantes_licitacao/delta"
    write_deltalake(
        f"s3a://{BUCKET_TERMO_ADITIVO}", 
        participantes_licitacao, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )
