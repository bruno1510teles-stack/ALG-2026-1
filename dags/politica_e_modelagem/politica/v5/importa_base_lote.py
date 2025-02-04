# Carregando libs
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import os, pytz
from datetime import datetime
import time
import base64
import requests

def importa_base_pre_aprovado_lote (access_params=None,  **kwargs):

    print('Parte 1 - Iniciando importação da base...')

    minio_raw = Minio(
        "api-raw.alpe.com.br",
        access_key = 'B7q0avvSIpSdyGPXWnEC',
        secret_key = 'PhMhRQSQ6YJU8fn2qKhDLM017cQPrlCz1YbM8IwU'
    )

    # Connection validation
    try:
        # Try to list the buckets
        buckets = minio_raw.list_buckets()
        
        # If the connection was successful, print the buckests
        print("Conexão bem-sucedida. Lista de buckets disponíveis:")
        for bucket in buckets:
            print(bucket.name)
        
    except Exception as e:
            # If the connection was failed, print the error message
            print(f"Erro ao conectar ao MinIO: {e}")


    # Bucket and Folder_Destination
    BUCKET_SOURCE_RAW = "pre-aprovado-lote"
    FOLDER_DESTINATION_RAW = 'year=2025/month=2/day=4'
    file_name = 'Clientes Novos.reativações - Curitiba (hunting).xlsx'
    file_path = f'{FOLDER_DESTINATION_RAW}/{file_name}'


    # Uploading Excel File
    response = minio_raw.get_object(BUCKET_SOURCE_RAW, file_path)
    file_data = BytesIO(response.read())
    base_analisar = pd.read_excel(file_data, sheet_name= 'Planilha1')

    print('Parte 2 - Tratando dados...')

    base_analisar['CNPJ'] = base_analisar['CNPJ'].astype(str).str.zfill(14)
    base_analisar['cnpj_raiz'] = base_analisar['CNPJ'].str.slice(0, 8).str.zfill(8)

    print(f"Quantidade de CNPJs na base_analisar: {base_analisar.shape[0]}")


    return base_analisar.to_dict(orient='records')

