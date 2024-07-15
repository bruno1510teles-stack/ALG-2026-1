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
def socios_to_trusted(files_list_socios, access_params):

    # Variaveis Conexão
    BUCKET_SOURCE_RAW = "receita-federal"
    BUCKET_SOURCE_TRUSTED = "receita-federal"
    TRUSTED_FOLDER = "socios/"

    # Conectando na trusted
    client = Minio(
        access_params['endpoint_url_raw'],
        access_key = access_params['aws_access_key_id_raw'],
        secret_key = access_params['aws_secret_access_key_raw'],
    )
    
    header_inicial = ['CNPJ BÁSICO',
                  'IDENTIFICADOR DE SÓCIO',
                  'NOME DO SÓCIO OU RAZÃO SOCIAL',
                  'CNPJ/CPF DO SÓCIO',
                  'QUALIFICAÇÃO DO SÓCIO',
                  'DATA DE ENTRADA SOCIEDADE',
                  'PAIS',
                  'REPRESENTANTE LEGAL',
                  'NOME DO REPRESENTANTE',
                  'QUALIFICAÇÃO DO REPRESENTANTE LEGAL',
                  'FAIXA ETÁRIA']
    
    # Importando dados de socios
    print(f'IMPORTANDO socios {psutil.virtual_memory()._asdict()}')

    for file_name in files_list_socios:
        print(f"file_name: {file_name} {psutil.virtual_memory()._asdict()}")
        file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
        for df in pd.read_csv(BytesIO(file.data), sep=';', encoding = 'latin1', header = None, dtype=str,  chunksize=3000000):
            print(f'df importado {psutil.virtual_memory()._asdict()}')
            
            df.columns = header_inicial

            print(f'criando dicts: {psutil.virtual_memory()._asdict()}')
            
            faixa_etaria_dict = {'1': '0 a 12 anos',
                    '2': '13 a 20 anos',
                    '3': '21 a 30 anos',
                    '4': '31 a 40 anos',
                    '5': '41 a 50 anos',
                    '6': '51 a 60 anos',
                    '7': '61 a 70 anos',
                    '8': '71 a 80 anos',
                    '9': 'mais de 80 anos',
                    '0': None}

            identificador_socio_dict = {
            '1':'PESSOA JURÍDICA',
            '2': 'PESSOA FÍSICA',
            '3': 'ESTRANGEIRO'}

            print(f'aplicando dicts: {psutil.virtual_memory()._asdict()}')
            
            df['FAIXA ETÁRIA'] = df['FAIXA ETÁRIA'].map(faixa_etaria_dict)

            df['IDENTIFICADOR DE SÓCIO'] = df['IDENTIFICADOR DE SÓCIO'].map(identificador_socio_dict)
            
            print(f'tratando data: {psutil.virtual_memory()._asdict()}')
            
            df['DATA DE ENTRADA SOCIEDADE'] = df['DATA DE ENTRADA SOCIEDADE'].str[0:4] + '-' + df['DATA DE ENTRADA SOCIEDADE'].str[4:6] + '-' + df['DATA DE ENTRADA SOCIEDADE'].str[6:8]
            
            print(f'renomeando colunas: {psutil.virtual_memory()._asdict()}')
            
            renomeando = {'CNPJ BÁSICO': 'cnpj_raiz',
            'IDENTIFICADOR DE SÓCIO': 'identificador_socio',
            'NOME DO SÓCIO OU RAZÃO SOCIAL': 'nome/razao_social',
            'CNPJ/CPF DO SÓCIO': 'documento_socio',
            'QUALIFICAÇÃO DO SÓCIO': 'qualificacao_socio',
            'DATA DE ENTRADA SOCIEDADE': 'data_entrada_sociedade',
            'PAIS': 'pais',
            'REPRESENTANTE LEGAL': 'representante_legal',
            'NOME DO REPRESENTANTE': 'nome_representante',
            'QUALIFICAÇÃO DO REPRESENTANTE LEGAL': 'qualificacao_representante',
            'FAIXA ETÁRIA': 'faixa_etaria_socio'}
            
            df = df.rename(columns = renomeando)
            
            df['data_ref'] = str(datetime.now().year)[0:3] + file_name[-14:-13] + file_name[-13:-11]
            
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
            
            del df
            print(f"Gravado na Trused! {psutil.virtual_memory()._asdict()}")
