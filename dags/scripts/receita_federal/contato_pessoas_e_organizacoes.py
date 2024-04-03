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
def transform_receita_to_contato(files_list_estabelecimento, access_params):

    # Variaveis Conexão
    BUCKET_SOURCE_RAW = "receita-federal"
    RAW_ESTABELECIMENTO_FOLDER = "estabelecimentos/"
    BUCKET_SOURCE_TRUSTED = "pessoas-e-organizacoes"
    TRUSTED_FOLDER = "contato/"

    # Conectando na trusted
    client = Minio(
        access_params['endpoint_url_raw'],
        access_key = access_params['aws_access_key_id_raw'],
        secret_key = access_params['aws_secret_access_key_raw'],
    )
    
    # Importando dados de ESTABELECIMENTO
    print('IMPORTANDO ESTABELECIMENTO')
    print(f'ARQUIVOS: {files_list_estabelecimento}')

    #definindo colunas a serem utilizadas
    colunas_estabelecimentos = [0, 1, 2, 21, 22, 23, 24, 25, 26, 27]

    #definindo tipo das colunas para importacao
    dtypes_estabelecimentos = {0:'string', 1:'string', 2:'string',
                            21:'string', 22:'string', 23:'string',
                            24:'string', 25:'string', 26:'string',
                            27:'string'}
    indice = 1
    #Tratando um arquivo por vez
    for file_name in files_list_estabelecimento:
        print(f"file_name: {file_name}")
        file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
        for df in pd.read_csv(BytesIO(file.data), sep=';', 
                            encoding='latin1', low_memory=False, chunksize=3000000, dtype=dtypes_estabelecimentos, usecols=colunas_estabelecimentos, header=None):
        
            print(f"Coluna Renomeada! {psutil.virtual_memory()._asdict()}")
            #definindo nome das colunas para tratamento inicial
            nome_colunas_estabelecimentos = {0:'CNPJ BÁSICO',
                1:'CNPJ ORDEM',
                2:'CNPJ DV',
                21:'DDD 1',
                22:'TELEFONE 1',
                23:'DDD 2',
                24:'TELEFONE 2',
                25:'DDD DO FAX',
                26:'FAX',
                27:'CORREIO ELETRONICO'}

            df = df.rename(columns=nome_colunas_estabelecimentos)

            print(f"Coluna identificador criada! {psutil.virtual_memory()._asdict()}")
            #criando coluna de identificador juntando todas as colunas de documento
            df['identificador'] = df['CNPJ BÁSICO'] + df['CNPJ ORDEM'] + df['CNPJ DV']

            #TRATAMENTO TELEFONE/FAZ E DDD:
            print(f"Tratando telefone: {psutil.virtual_memory()._asdict()}")
            #TELEFONE1 E DD1
            #Caso nem DDD nem Telefone são nulos
            mask1 = df['DDD 1'].notna()
            mask2 = df['TELEFONE 1'].notna()
            df.loc[mask1 & mask2, 'TELEFONE 1'] = df.loc[mask1 & mask2, 'DDD 1'] + df.loc[mask1 & mask2, 'TELEFONE 1']

            #Caso nem DDD Nulo nem Telefone não nulo
            #mask1 = df['DDD 1'].isna()
            #mask2 = df['TELEFONE 1'].notna()
            # Não faz nada pois só manteremos o telefone na coluna
            # df.loc[mask1 & mask2, 'TELEFONE 1'] =  df.loc[mask1 & mask2, 'TELEFONE 1']

            #Caso em que o telefone é Nulo (independente da existência do DDD nulo ou não)
            mask1 = df['TELEFONE 1'].isna()
            df.loc[mask1, 'TELEFONE 1'] =  None


            #TELEFONE2 E DDD2
            #   Caso nem DDD nem Telefone são nulos
            mask1 = df['DDD 2'].notna()
            mask2 = df['TELEFONE 2'].notna()
            df.loc[mask1 & mask2, 'TELEFONE 2'] = df.loc[mask1 & mask2, 'DDD 2'] + df.loc[mask1 & mask2, 'TELEFONE 2']

            #   Caso nem DDD Nulo nem Telefone não nulo
            #mask1 = df['DDD 2'].isna()
            #mask2 = df['TELEFONE 2'].notna()
            # Não faz nada pois só manteremos o telefone na coluna
            # df.loc[mask1 & mask2, 'TELEFONE 2'] =  df.loc[mask1 & mask2, 'TELEFONE 2']

            #   Caso em que o telefone é Nulo (independente da existência do DDD nulo ou não)
            mask1 = df['TELEFONE 2'].isna()
            df.loc[mask1, 'TELEFONE 2'] =  None


            #FAX:
            #   Caso nem DDD nem FAX são nulos
            mask1 = df['DDD DO FAX'].notna()
            mask2 = df['FAX'].notna()
            df.loc[mask1 & mask2, 'FAX'] = df.loc[mask1 & mask2, 'DDD 2'] + df.loc[mask1 & mask2, 'FAX']

            #   Caso nem DDD Nulo nem FAX não nulo
            #mask1 = df['DDD 2'].isna()
            #mask2 = df['TELEFONE 2'].notna()
            # Não faz nada pois só manteremos o telefone na coluna
            # df.loc[mask1 & mask2, 'TELEFONE 2'] =  df.loc[mask1 & mask2, 'TELEFONE 2']

            #   Caso em que o telefone é Nulo (independente da existência do DDD nulo ou não)
            mask1 = df['DDD DO FAX'].isna()
            df.loc[mask1, 'FAX'] =  None


            df = df.drop(columns=['CNPJ BÁSICO', 'CNPJ ORDEM', 'CNPJ DV', 'DDD 1', 'DDD 2', 'DDD DO FAX'])

            #Transformando em nulo casos em que o Telefone 1 for igual ao telefone 2
            df.loc[df['TELEFONE 1'] == df['TELEFONE 2'], 'TELEFONE 2'] = None

            del mask1
            del mask2

            print(f"Criando coluna única: {psutil.virtual_memory()._asdict()}")            
            
            # Padronizando todos os valores de contatato em única coluna
            df = df.melt(id_vars='identificador', value_vars=['TELEFONE 1', 'TELEFONE 2', 'FAX', 'CORREIO ELETRONICO'], 
                                value_name='valor')

            #Dropando valores de contato que são nulos
            df = df.dropna(subset = ['valor'])

            #renomeando coluna de tipo
            df = df.rename(columns={'variable': 'tipo'})

            #renomeando registros da coluna de valor
            valor_coluna_tipo = {'TELEFONE 1': 'TELEFONE',
            'TELEFONE 2': 'TELEFONE',
            'CORREIO ELETRONICO': 'EMAIL',
            'FAX': 'FAX'}

            df['tipo'] = df['tipo'].map(valor_coluna_tipo)

            #reordenando colunas
            df = df[['identificador', 'valor', 'tipo']]
            
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
            len(files_list_estabelecimento)
            print(f'Processamento Concluido: {indice/len(files_list_estabelecimento)}%')
            indice += 1
