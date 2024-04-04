# Carregando libs
import pandas as pd
import pyarrow as pa
import copy
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
import os
from deltalake import write_deltalake, DeltaTable
import psutil


# Criando conexão
def transform_socios_receita_to_documento(files_list_socio, access_params):

    # Variaveis Conexão
    BUCKET_SOURCE_RAW = "receita-federal"
    BUCKET_SOURCE_TRUSTED = "pessoas-e-organizacoes"
    TRUSTED_FOLDER = "documento/"

    # Conectando na trusted
    client = Minio(
        access_params['endpoint_url_raw'],
        access_key = access_params['aws_access_key_id_raw'],
        secret_key = access_params['aws_secret_access_key_raw'],
    )
    
    # Importando dados de SOCIOS
    print('IMPORTANDO SOCIOS')

    #definindo colunas e tipos que serão importandos
    colunas_socios = [1, 3]
    dtypes_socios = 'str'
            

    for file_name in files_list_socio:
        print(f"file_name: {file_name} {psutil.virtual_memory()._asdict()}")
        file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
        df_socios = pd.read_csv(BytesIO(file.data), sep=';', encoding = 'latin1', dtype=dtypes_socios, usecols=colunas_socios, header=None)

        print(f"Renomeando colunas: {psutil.virtual_memory()._asdict()}")   
        nome_colunas = {1: 'tipo', 
        3: 'identificador'}

        df_socios = df_socios.rename(columns=nome_colunas)
        
        #criando coluna valor
        df_socios['valor'] = df_socios['identificador']
        
        #alterando valores coluna tipo
        dict_documento = {'1': 'CNPJ',
                '2': 'CPF',
                '3': 'ESTRANGEIRO'}
        
        df_socios['tipo'] = df_socios['tipo'].map(dict_documento)
        
        #reoordenando colunas
        df_socios = df_socios[['identificador', 'tipo', 'valor']]

        df_socios['fonte'] = 'RECEITA FEDERAL'
        df_socios['fonte'] = df_socios['fonte'].astype(str)
        
        print(f"Ordenando colunas: {psutil.virtual_memory()._asdict()}")   

        #Definindo data de tratamento do arquivo
        now = datetime.now(tz=timezone(timedelta(hours=-3)))
        
        df_socios['year'] = now.year
        df_socios['month'] = now.month
        df_socios['day'] = now.day
        
        storage_options = {
            "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
            "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
            "AWS_ENDPOINT_URL":f"https://{access_params['endpoint_url_trusted']}",
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }
        
        print(f"Transformando em Pyarrow! {psutil.virtual_memory()._asdict()}")
        # O pandas cria esse index, este codigo serve para remover caso ele crie
        df_socios = pa.Table.from_pandas(df_socios, preserve_index=False)

        print(f"Gravando na Trused! {psutil.virtual_memory()._asdict()}")
        write_deltalake(f"s3a://{BUCKET_SOURCE_TRUSTED}/{TRUSTED_FOLDER}", 
                        df_socios, 
                        partition_by=["year", "month", "day"],
                        storage_options=storage_options,
                        mode="append",
                        )
        
        del df_socios
        print(f"Gravado na Trused! {psutil.virtual_memory()._asdict()}")
