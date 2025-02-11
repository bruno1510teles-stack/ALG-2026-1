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
import numpy as np


# Criando conexão
def transform_receita_to_endereco(files_list_estabelecimento, files_list_endereco, access_params):

    # Variaveis Conexão
    BUCKET_SOURCE_RAW = "receita-federal"
    BUCKET_SOURCE_TRUSTED = "pessoas-e-organizacoes"
    TRUSTED_FOLDER = "endereco/"

    # Conectando na trusted
    client = Minio(
        access_params['endpoint_url_raw'],
        access_key = access_params['aws_access_key_id_raw'],
        secret_key = access_params['aws_secret_access_key_raw'],
    )
    
    #Como é apenas um arquivo de endereco por vez não é necessário concatenar DF como os outros tratamentos
    print('IMPORTANDO MUNICIPIOS')
    print(f'Lista de arquivos a serem processados: {files_list_endereco}')
    
    file_name = files_list_endereco[0]
    file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
    df_municipio = pd.read_csv(BytesIO(file.data), dtype='str', sep=';', header=None, encoding='latin1')

    #transformando df em dict para posterior map
    dict_municipio = dict(zip(df_municipio[0], df_municipio[1]))
    del df_municipio
    
    # Importando dados de ESTABELECIMENTO
    print('IMPORTANDO ESTABELECIMENTO')

    #definindo colunas a serem utilizadas
    colunas_estabelecimentos = [0, 1, 2, 13, 14, 15, 16, 17, 18, 19, 20]

    #definindo tipo das colunas para importacao
    dtypes_estabelecimentos = {0:'string', 1:'string', 2:'string', 13:'string', 14:'string',
                                15:'string', 16:'string', 17:'string', 18:'string', 19:'string',
                                20:'string'}

    print(f"Estabelecimentos: {files_list_estabelecimento}")
    #Tratando um arquivo por vez
    for file_name in files_list_estabelecimento:
        print(f"file_name: {file_name}")

        file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
        for df in pd.read_csv(BytesIO(file.data), sep=';', 
                            encoding='latin1', low_memory=False, chunksize=4000000, dtype=dtypes_estabelecimentos, usecols=colunas_estabelecimentos, header=None):

            print(f"renomeando colunas {psutil.virtual_memory()._asdict()}")
            #definindo nome das colunas para tratamento inicial
            nome_colunas_estabelecimentos = {0:'CNPJ BÁSICO',
                1:'CNPJ ORDEM',
                2:'CNPJ DV',
                13:'tipo logradouro', 
                14:'logradouro',
                15:'numero',
                16:'complemento',
                17:'bairro', 
                18:'cep', 
                19:'uf',
                20:'municipio'}

            df = df.rename(columns=nome_colunas_estabelecimentos)

            #criando coluna de identificador juntando todas as colunas de documento
            df['identificador'] = df['CNPJ BÁSICO'] + df['CNPJ ORDEM'] + df['CNPJ DV']

            #juntando tipo de logradouro e logradouro em única coluna
            df['logradouro'] = df['tipo logradouro'] + ' ' + df['logradouro'] 

            # dropando colunas descenecessárias
            df = df.drop(columns=['CNPJ BÁSICO', 'CNPJ ORDEM', 'CNPJ DV', 'tipo logradouro'])

            print(f"substituindo chave valor de municipio {psutil.virtual_memory()._asdict()}")
            #substituindo código do munícipio pela descrição do município
            df['municipio'] = df['municipio'].map(dict_municipio)

            #reoordenando colunas 
            df = df[['identificador', 'logradouro', 'numero', 'complemento', 'bairro', 'municipio', 'cep', 'uf']]

            df['fonte'] = 'RECEITA FEDERAL'

            #Definindo data de tratamento do arquivo
            now = datetime.now(tz=timezone(timedelta(hours=-3)))
            
            df['year'] = now.year
            df['month'] = now.month
            df['day'] = now.day
                
            storage_options = {
                "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
                "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
                "AWS_ENDPOINT_URL":f"https://{access_params['endpoint_url_trusted']}",
                "AWS_REGION": "us-east-1",
                "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            }
            
            print(f"Transformando em Pyarrow! {psutil.virtual_memory()._asdict()}")
            # O pandas cria esse index, este codigo serve para remover caso ele crie
            df = pa.Table.from_pandas(df, preserve_index=False)

            print(f"Gravando na Trused! {psutil.virtual_memory()._asdict()}")
            write_deltalake(f"s3a://{BUCKET_SOURCE_TRUSTED}/{TRUSTED_FOLDER}", 
                            df, 
                            partition_by=["year", "month", "day"],
                            storage_options=storage_options,
                            mode="append",
                            )
            
            print("Gravado na Trused!")
