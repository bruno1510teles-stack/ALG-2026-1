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
def transform_socios_receita_to_organizacoes(files_list_socio, access_params):

    # Variaveis Conexão
    BUCKET_SOURCE_RAW = "receita-federal"
    BUCKET_SOURCE_TRUSTED = "pessoas-e-organizacoes"
    TRUSTED_FOLDER = "organizacoes/"

    # Conectando na trusted
    client = Minio(
        access_params['endpoint_url_raw'],
        access_key = access_params['aws_access_key_id_raw'],
        secret_key = access_params['aws_secret_access_key_raw'],
    )
    
    # Importando dados de SOCIOS
    print('IMPORTANDO SOCIOS')

    #definindo colunas a serem utilizadas
    colunas_socios = [0, 1, 2, 5, 6]

    #definindo tipo das colunas para importacao
    dtypes_socios = {0:'string', 1:'string', 2:'string', 5:'string', 6:'string'}
    
    dfs = []

    #Tratando um arquivo por vez
    for file_name in files_list_socio:
        print(f"file_name: {file_name} {psutil.virtual_memory()._asdict()}")
        file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
        df_inter = pd.read_csv(BytesIO(file.data), sep=';', encoding = 'latin1', dtype=dtypes_socios, usecols=colunas_socios, header=None)
        dfs.append(df_inter)
        print(f"arquivo lido: {file_name} {psutil.virtual_memory()._asdict()}")
     
    print(f"CONCATENANDO ARQUIVOS: {psutil.virtual_memory()._asdict()}")    
    df_socios = pd.concat(dfs, ignore_index=True)
    
    print(f"CONCATENADOS: {psutil.virtual_memory()._asdict()}")    
    #Alterando nome das colunas
    nome_colunas = {2: 'identificador', 
    3: 'nome', 
    5: 'inicio', 
    10: 'idade'}

    df_socios = df_socios.rename(columns=nome_colunas)
    
    #Criando colunas que não vem originalmente no DF
    df_socios['razao social'] = None
    df_socios['razao social'].astype(str)

    df_socios['capital'] = None
    df_socios['capital'].astype(str)

    df_socios['tamanho'] = None
    df_socios['tamanho'].astype(str)

    df_socios['fonte'] = 'RECEITA FEDERAL'
    df_socios['fonte'].astype(str)
    
    #alterando ordem das colunas
    df_socios = df_socios[['identificador', 'nome', 'razao social', 'inicio', 'capital', 'tamanho', 'fonte', 'idade']]
    
    #Substituindo código de idade com descrição
    dict_idades = {"1": '0 a 12 anos', 
        "2": '13 a 20 anos',
        "3": '21 a 30 anos',
        "4": '31 a 40 anos',
        "5": '41 a 50 anos',
        "6": '51 a 60 anos',
        "7": '61 a 70 anos',
        "8": '71 a 80 anos',
        "9": '> 80 anos',
        "0": 'não se aplica'}

    df_socios['idade'] = df_socios['idade'].map(dict_idades)


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
    
    print("Transformando em Pyarrow!")
    # O pandas cria esse index, este codigo serve para remover caso ele crie
    df_socios = pa.Table.from_pandas(df_socios, preserve_index=False)

    print("Gravando na Trused!")
    write_deltalake(f"s3a://{BUCKET_SOURCE_TRUSTED}/{TRUSTED_FOLDER}", 
                    df_socios, 
                    partition_by=["year", "month", "day"],
                    storage_options=storage_options,
                    mode="append",
                    )
    
    print("Gravado na Trused!")
