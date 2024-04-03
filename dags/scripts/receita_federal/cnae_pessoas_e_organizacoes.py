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
def transform_receita_to_cnae(files_list_estabelecimento, files_list_cnae, access_params):

    # Variaveis Conexão
    BUCKET_SOURCE_RAW = "receita-federal"
    RAW_ESTABELECIMENTO_FOLDER = "estabelecimentos/"
    RAW_CNAE_FOLDER = "cnae/"
    BUCKET_SOURCE_TRUSTED = "pessoas-e-organizacoes"
    TRUSTED_FOLDER = "cnae/"

    # Conectando na trusted
    client = Minio(
        access_params['endpoint_url_raw'],
        access_key = access_params['aws_access_key_id_raw'],
        secret_key = access_params['aws_secret_access_key_raw'],
    )
    df_cnae = None
    
    #Como é apenas um arquivo de CNAE por vez não é necessário concatenar DF como os outros tratamentos
    print('IMPORTANDO CNAES')
    print(f'Lista de arquivos a serem processados: {files_list_cnae}')
    for file_name in files_list_cnae:
        print(f"file_name: {file_name}")
        file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
        df_cnae = pd.read_csv(BytesIO(file.data), dtype='str', sep=';', header=None, encoding='latin1')

    dict_cnaes = dict(zip(df_cnae[0], df_cnae[1]))
    
    # Importando dados de ESTABELECIMENTO
    print('IMPORTANDO ESTABELECIMENTO')

    #definindo colunas a serem utilizadas
    colunas_estabelecimentos = [0, 1, 2, 21, 22, 23, 24, 25, 26, 27]

    #definindo tipo das colunas para importacao
    dtypes_estabelecimentos = {0:'string', 1:'string', 2:'string',
                            21:'string', 22:'string', 23:'string',
                            24:'string', 25:'string', 26:'string',
                            27:'string'}

    
    #Tratando um arquivo por vez
    for file_name in files_list_estabelecimento:
        print(f"file_name: {file_name}")

        file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
        for df in pd.read_csv(BytesIO(file.data), sep=';', 
                            encoding='latin1', low_memory=False, chunksize=6000000, dtype=dtypes_estabelecimentos, usecols=colunas_estabelecimentos, header=None):
        
            
            #definindo colunas a serem utilizadas
            colunas_estabelecimentos = [0, 1, 2, 11, 12]

            #definindo tipo das colunas para importacao
            dtypes_estabelecimentos = {0:'string', 1:'string', 2:'string', 11:'string', 12:'string'}

            df = pd.read_csv(r'C:\Users\joao.leite\OneDrive - Yandeh\Desktop\receita-federal\estabelecimentos\K3241.K03200Y1.D40210.ESTABELE', sep=';', dtype=dtypes_estabelecimentos, usecols=colunas_estabelecimentos, encoding='latin1', nrows = 2000, low_memory=False, header=None)

            #definindo nome das colunas para tratamento inicial
            nome_colunas_estabelecimentos = {0:'CNPJ BÁSICO',
                1:'CNPJ ORDEM',
                2:'CNPJ DV',
                11:'CNAE FISCAL PRINCIPAL',
                12:'CNAE FISCAL SECUNDARIA'}

            df = df.rename(columns=nome_colunas_estabelecimentos)

            #criando coluna de identificador juntando todas as colunas de documento
            df['identificador'] = df['CNPJ BÁSICO'] + df['CNPJ ORDEM'] + df['CNPJ DV']

            #criando coluna de código cnae
            df['CODIGO CNAE PRINCIPAL'] = df['CNAE FISCAL PRINCIPAL']
            df['CODIGO CNAE SECUNDARIA'] = df['CNAE FISCAL SECUNDARIA']

            #separando string de CNEAS secundários em linhas diferentes
            df['CODIGO CNAE SECUNDARIA'] = df['CODIGO CNAE SECUNDARIA'].str.split(',')
            df = df.explode('CODIGO CNAE SECUNDARIA')

            #Como o código anterior dupliaca o numero de cnaes principais, ao deixar cada CNAE SECUNDARIA em uma linha, anulamos os cnaes principais duplicados para posterior remoção.
            df['CODIGO CNAE PRINCIPAL'] = np.where(df.duplicated(subset=['identificador', 'CODIGO CNAE PRINCIPAL']), np.nan, df['CODIGO CNAE PRINCIPAL'])

            #eliminando colunas desnecessárioas após o tratamento
            df = df.drop(columns=['CNPJ BÁSICO', 'CNPJ ORDEM', 'CNPJ DV', 'CNAE FISCAL PRINCIPAL', 'CNAE FISCAL SECUNDARIA'])

            #deixando um CNAE por linha
            df = df.melt(id_vars='identificador', value_vars=['CODIGO CNAE PRINCIPAL', 'CODIGO CNAE SECUNDARIA'], 
                            value_name='codigo')

            #usando dict para renomear os códigos de CNAES
            df['descricao'] = df['codigo'].map(dict_cnaes)

            df = df.rename(columns={'variable': 'tipo'})

            #ordenando colunas
            df = df[['identificador', 'codigo', 'descricao', 'tipo']]

            #alterando registro da coluna tipo
            dict_tipo = {'CODIGO CNAE PRINCIPAL': 'PRIMÁRIO',
                        'CODIGO CNAE SECUNDARIA': 'SECUNDÁRIO'}
            df['tipo'] = df['tipo'].map(dict_tipo)

            #eliminando CNAES nulos criados no tratamento de duplicatas
            df = df.dropna(subset='codigo')

            
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
            
            print("Transformando em Pyarrow!")
            # O pandas cria esse index, este codigo serve para remover caso ele crie
            df = pa.Table.from_pandas(df, preserve_index=False)

            print("Gravando na Trused!")
            write_deltalake(f"s3a://{BUCKET_SOURCE_TRUSTED}/{TRUSTED_FOLDER}", 
                            df, 
                            partition_by=["year", "month", "day"],
                            storage_options=storage_options,
                            mode="append",
                            )
            
            print("Gravado na Trused!")
