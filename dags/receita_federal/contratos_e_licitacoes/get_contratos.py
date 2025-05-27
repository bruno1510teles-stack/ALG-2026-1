# Importing Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake
import unicodedata
import re

def get_bases_contratos (access_params=None):

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
    FOLDER_DESTINATION_RAW = 'contratos'

    print('Carregando dados Compras')

    # Compras
    file_compras = '202501_Compras.csv'
    path_compras = f'{FOLDER_DESTINATION_RAW}/{file_compras}'

    response_compras = minio_trusted.get_object(BUCKET_SOURCE_RAW, path_compras)
    file_data_compras = BytesIO(response_compras.read())
    compras = pd.read_csv(file_data_compras, sep=';', quotechar='"', encoding='latin1')

    print('Tamanho da base Compras:')
    print(len(compras))



    print('Carregando dados Item Compra')

    # Item Compra
    file_item_compra = '202501_ItemCompra.csv'
    path_item_compra = f'{FOLDER_DESTINATION_RAW}/{file_item_compra}'

    response_item_compra = minio_trusted.get_object(BUCKET_SOURCE_RAW, path_item_compra)
    file_data_item_compra = BytesIO(response_item_compra.read())
    item_compra = pd.read_csv(file_data_item_compra, sep=';', quotechar='"', encoding='latin1')

    print('Tamanho da base Item Compra:')
    print(len(item_compra))



    print('Carregando dados Termo Aditivo')

    # Termo Aditivo
    file_termo_aditivo = '202501_TermoAditivo.csv'
    path_termo_aditivo = f'{FOLDER_DESTINATION_RAW}/{file_termo_aditivo}'

    response_termo_aditivo = minio_trusted.get_object(BUCKET_SOURCE_RAW, path_termo_aditivo)
    file_data_termo_aditivo = BytesIO(response_termo_aditivo.read())
    termo_aditivo = pd.read_csv(file_data_termo_aditivo, sep=';', quotechar='"', encoding='latin1')

    print('Tamanho da base Termo Aditivo:')
    print(len(termo_aditivo))



    def normalizar_colunas(col):
        col = unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('utf-8')

        col = col.replace(' ', '_')
        
        col = re.sub(r'[^\w_]', '', col)

        return col.lower()
    

    # Tratando nome das colunas de todos os Dataframes

    compras.columns = [normalizar_colunas(col) for col in compras.columns]

    item_compra.columns = [normalizar_colunas(col) for col in item_compra.columns]

    termo_aditivo.columns = [normalizar_colunas(col) for col in termo_aditivo.columns]



    now = datetime.now(tz=timezone(timedelta(hours=-3)))

    compras['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    compras['year'], compras['month'], compras['day'] = now.year, now.month, now.day
    compras = compras.reset_index(drop=True)

    item_compra['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    item_compra['year'], item_compra['month'], item_compra['day'] = now.year, now.month, now.day
    item_compra = item_compra.reset_index(drop=True)

    termo_aditivo['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    termo_aditivo['year'], termo_aditivo['month'], termo_aditivo['day'] = now.year, now.month, now.day
    termo_aditivo = termo_aditivo.reset_index(drop=True)

    # Exportando 

    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # -- Exportação dos dados com validação --


    print('Exportando dados Compras...')
    BUCKET_COMPRAS = "receita-federal/contratos/compras/delta"
    write_deltalake(
        f"s3a://{BUCKET_COMPRAS}", 
        compras, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )


    print('Exportando dados Item Compra...')
    BUCKET_ITEM_COMPRA = "receita-federal/contratos/item_compra/delta"
    write_deltalake(
        f"s3a://{BUCKET_ITEM_COMPRA}", 
        item_compra, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )


    print('Exportando dados Termo Aditivo...')
    BUCKET_TERMO_ADITIVO = "receita-federal/contratos/termo_aditivo/delta"
    write_deltalake(
        f"s3a://{BUCKET_TERMO_ADITIVO}", 
        termo_aditivo, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )

