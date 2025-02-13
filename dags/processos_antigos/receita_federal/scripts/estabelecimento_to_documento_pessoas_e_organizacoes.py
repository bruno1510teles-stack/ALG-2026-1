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

print('Working')

# Criando conexão
def transform_estabelecimento_receita_to_documento(files_list_estabelecimento, access_params):

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
    
    # Importando dados de ESTABELECIMENTOS
    print('IMPORTANDO ESTABELECIMENTOS')

    #definindo colunas e tipos que serão importandos
    colunas_estabelecimentos = [0, 1, 2]
    dtypes_estabelecimentos = 'str'
            

    for file_name in files_list_estabelecimento:
        print(f"file_name: {file_name} {psutil.virtual_memory()._asdict()}")
        file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
        for df_estabelecimentos in pd.read_csv(BytesIO(file.data), sep=';', encoding = 'latin1', dtype=dtypes_estabelecimentos, usecols=colunas_estabelecimentos, header=None, chunksize=6000000):

            (f"Criando coluna identificador {psutil.virtual_memory()._asdict()}")
            df_estabelecimentos['identificador'] = df_estabelecimentos[0] + df_estabelecimentos[1] + df_estabelecimentos[2]
            
            (f"Criando coluna tipo {psutil.virtual_memory()._asdict()}")
            df_estabelecimentos['tipo'] = 'CNPJ'
            
            (f"Criando coluna valor {psutil.virtual_memory()._asdict()}")
            df_estabelecimentos['valor'] = df_estabelecimentos['identificador']

            (f"Criando coluna fonte {psutil.virtual_memory()._asdict()}")
            df_estabelecimentos['fonte'] = 'RECEITA FEDERAL'
            df_estabelecimentos['fonte'] = df_estabelecimentos['fonte'].astype(str)

            (f"Criando colunas de particionamento {psutil.virtual_memory()._asdict()}")
            #Definindo data de tratamento do arquivo
            now = datetime.now(tz=timezone(timedelta(hours=-3)))
            
            df_estabelecimentos['year'] = now.year
            df_estabelecimentos['month'] = now.month
            df_estabelecimentos['day'] = now.day
            
            #Dropando colunas desnecessárias
            
            df_estabelecimentos = df_estabelecimentos[['identificador', 'tipo', 'valor', 'fonte', 'year', 'month', 'day']]
            
            storage_options = {
                "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
                "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
                "AWS_ENDPOINT_URL":f"https://{access_params['endpoint_url_trusted']}",
                "AWS_REGION": "us-east-1",
                "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            }
            
            print(f"Transformando em Pyarrow! {psutil.virtual_memory()._asdict()}")
            # O pandas cria esse index, este codigo serve para remover caso ele crie
            df_estabelecimentos = pa.Table.from_pandas(df_estabelecimentos, preserve_index=False)

            print(f"Gravando na Trused! {psutil.virtual_memory()._asdict()}")
            write_deltalake(f"s3a://{BUCKET_SOURCE_TRUSTED}/{TRUSTED_FOLDER}", 
                            df_estabelecimentos, 
                            partition_by=["year", "month", "day"],
                            storage_options=storage_options,
                            mode="append",
                            )
            
            del df_estabelecimentos
            print(f"Gravado na Trused! {psutil.virtual_memory()._asdict()}")
