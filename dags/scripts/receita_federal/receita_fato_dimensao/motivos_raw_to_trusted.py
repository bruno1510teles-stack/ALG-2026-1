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
def motivos_to_trusted(files_list_motivos, access_params):

    # Variaveis Conexão
    BUCKET_SOURCE_RAW = "receita-federal"
    BUCKET_SOURCE_TRUSTED = "receita-federal"
    TRUSTED_FOLDER = "motivos/"

    # Conectando na trusted
    client = Minio(
        access_params['endpoint_url_raw'],
        access_key = access_params['aws_access_key_id_raw'],
        secret_key = access_params['aws_secret_access_key_raw'],
    )
    
    header_inicial = ['codigo',
                    'descricao']

    
    # Importando dados de MOTIVOS
    print(f'IMPORTANDO MOTIVOS {psutil.virtual_memory()._asdict()}')

    for file_name in files_list_motivos:
        print(f"file_name: {file_name} {psutil.virtual_memory()._asdict()}")
        file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
        df = pd.read_csv(BytesIO(file.data), sep=';', encoding = 'latin1', header = None, dtype=str)
        print(f'df importado {psutil.virtual_memory()._asdict()}')
            
        df.columns = header_inicial

        df['data_ref'] = str(datetime.now().year)[0:3] + file_name[-13:-12] + file_name[-12:-10]

        (f"Criando colunas de particionamento {psutil.virtual_memory()._asdict()}")
        #Definindo data de tratamento do arquivo
        now = datetime.now(tz=timezone(timedelta(hours=-3)))
        
        df['atualizado_em'] = now.strftime('%Y-%m-%d %X')  
        
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

        print(f"Gravado na Trused! {psutil.virtual_memory()._asdict()}")
