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
def transform_receita_to_relacionamento(files_list_estabelecimento, files_list_empresa, access_params):

    # Variaveis Conexão
    BUCKET_SOURCE_RAW = "receita-federal"
    BUCKET_SOURCE_TRUSTED = "pessoas-e-organizacoes"
    TRUSTED_FOLDER = "relacionamento/"

    # Conectando na trusted
    client = Minio(
        access_params['endpoint_url_raw'],
        access_key = access_params['aws_access_key_id_raw'],
        secret_key = access_params['aws_secret_access_key_raw'],
    )


    print('IMPORTANDO SOCIOS')
    # Importando dados de SOCIOS
    
    # Definindo colunas a serem utilizadas e respectivos tipos
    dtype_socios = {
            0:'string',
            1:'string',
            3:'string'
        }
    cols = [0, 1, 3]   
        
    #Loop para importar um arquivo de empresa por vez para diminuir a quanitdade de memoria utilizada    
    for file_name in files_list_empresa:
        print(f"file_name: {file_name}")
        file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
        df_socios = pd.read_csv(BytesIO(file.data), sep=';', 
                            encoding='latin1', low_memory=False,  dtype=dtype_socios, usecols=cols, header=None)


        # Renomeando colunas com o nome padrão da Recita
        header_socios = {0: 'CNPJ BÁSICO',
                 1: 'IDENTIFICADOR DE SÓCIO',
                 3: 'CNPJ/CPF DO SÓCIO'}
        
        df_socios = df_socios.rename(columns=header_socios)
        
        # Retirando Estrangeiros pois estes não entraram na relacao de organizações
        df_socios = df_socios[df_socios['IDENTIFICADOR DE SÓCIO'] != '3']
        
        # Substituindo os valores da coluna 'IDENTIFICADOR DE SÓCIO' de acordo com o dicionário
        de_para_tipo_documento = {"1": 'PESSOA JURÍDICA',
                          "2": "PESSOA FÍSICA"}
        
        df_socios['IDENTIFICADOR DE SÓCIO'] = df_socios['IDENTIFICADOR DE SÓCIO'].map(de_para_tipo_documento)
        
        # Importando dados de ESTABELECIMENTO
        print('IMPORTANDO ESTABELECIMENTO')
        # Definindo tipo da coluna e colunas a serem utilizadas no estabelecimento
        dtype_estabelecimento = {
                0:'string',
                1:'string',
                2:'string'
            }
        cols = [0, 1, 2]   
        
        #Dentro do loop de empresa é feito um loop de estabelecimentos no qual, cada arquivo de estabelecimento vai ser divido em chunks e porcessado por partes
        for file_name in files_list_estabelecimento:
            print(f"file_name: {file_name}")
            file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
            for df_estabelecimento in pd.read_csv(BytesIO(file.data), sep=';', 
                                encoding='latin1', low_memory=False, chunksize=6000000, dtype=dtype_estabelecimento, usecols=cols, header=None):
            

                print(f"Coluna Renomeada! {psutil.virtual_memory()._asdict()}")
                #renomeando colunas conforme padrão da Receita
                header_estabelecimento = {0: 'CNPJ BÁSICO',
                                1: 'CNPJ ORDEM',
                                2: 'CNPJ DV'}
                
                df_estabelecimento = df_estabelecimento.rename(columns=header_estabelecimento)
                
                
                # CRIANDO A TABELA DE RELACIONAMENTO:
                print(f"Merge com Sócios {psutil.virtual_memory()._asdict()}")
                df_relacionamento = df_estabelecimento.merge(df_socios, how='inner',  on='CNPJ BÁSICO')
                
                print(f"Excluindo dfs intermediários {psutil.virtual_memory()._asdict()}")
                #Excluindo df_intermediários
                del df_estabelecimento
                del df_socios
                
                print(f"Criando identificador {psutil.virtual_memory()._asdict()}")
                df_relacionamento['documento_estabelecimento'] = df_relacionamento['CNPJ BÁSICO'] + df_relacionamento['CNPJ ORDEM'] + df_relacionamento['CNPJ DV']

                df_relacionamento = df_relacionamento.drop(columns=['CNPJ BÁSICO', 'CNPJ ORDEM', 'CNPJ DV'])
                
                print(f"Criando tipo e fonte {psutil.virtual_memory()._asdict()}")
                df_relacionamento['tipo'] = 'SÓCIO'
                df_relacionamento['fonte'] = 'RECEITA FEDERAL'
                
                print(f"Ordenando colunas {psutil.virtual_memory()._asdict()}")
                #Ordenando Colunas
                df_relacionamento = df_relacionamento[['documento_estabelecimento', 'CNPJ/CPF DO SÓCIO', 'IDENTIFICADOR DE SÓCIO', 'tipo', 'fonte']]
                
                print(f"Renomeando colunas {psutil.virtual_memory()._asdict()}")
                #renomeando colunas
                df_relacionamento.columns = ['identificador', 'contraparte', 'classificação', 'tipo', 'fonte']
                
                
                print(f"Criando colunas de particionamento {psutil.virtual_memory()._asdict()}")
                #Definindo data de tratamento do arquivo
                now = datetime.now(tz=timezone(timedelta(hours=-3)))
                
                df_relacionamento['year'] = now.year
                df_relacionamento['month'] = now.month
                df_relacionamento['day'] = now.day

                    
                storage_options = {
                    "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
                    "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
                    "AWS_ENDPOINT_URL":f"https://{access_params['endpoint_url_trusted']}",
                    "AWS_REGION": "us-east-1",
                    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
                }
                
                #Definindo schema para salvar no PyArrow
                schema = pa.schema([
                    ('identificador', pa.string()),
                    ('contraparte', pa.string()),
                    ('classificação', pa.string()),
                    ('tipo', pa.string()),
                    ('fonte', pa.string()),
                    ('year', pa.int32()),
                    ('month', pa.int32()),
                    ('day', pa.int32())
                ])
                
                print(f"Transformando em Pyarrow! {psutil.virtual_memory()._asdict()}")
                # O pandas cria esse index, este codigo serve para remover caso ele crie
                df_relacionamento = pa.Table.from_pandas(df_relacionamento, preserve_index=False, schema=schema)

                print(f"Gravando Relacionamento 1 na Trused! {psutil.virtual_memory()._asdict()}")
                write_deltalake(f"s3a://{BUCKET_SOURCE_TRUSTED}/{TRUSTED_FOLDER}", 
                                df_relacionamento, 
                                partition_by=["year", "month", "day"],
                                storage_options=storage_options,
                                mode="append",
                                )
                
                
                print(f"Gravado Relacionamento 1 na Trused!{psutil.virtual_memory()._asdict()}")
                
                
                #Gravando o relacionamento inverso
                print(f"Gravando Relacionamento 2 na Trused!{psutil.virtual_memory()._asdict()}")
                
                nova_ordem = ['contraparte', 'identificador', 'classificação', 'tipo', 'fonte', 'year', 'month', 'day']
                
                df_relacionamento = df_relacionamento.select(nova_ordem)
                
                # Dicionário de mapeamento de nome antigo para nome novo
                column_rename_mapping = {'contraparte': 'identificador', 'identificador': 'contraparte'}

                # Renomeia as colunas usando o dicionário de mapeamento
                df_relacionamento = df_relacionamento.rename_columns(column_rename_mapping)
                
                df_relacionamento.columns = ['identificador', 'contraparte', 'classificação', 'tipo', 'fonte', 'year', 'month', 'day']
                
                df_relacionamento = df_relacionamento.set_column('tipo', pa.array(['ACIONISTA'] * len(df_relacionamento), type='str'))
                

                write_deltalake(f"s3a://{BUCKET_SOURCE_TRUSTED}/{TRUSTED_FOLDER}", 
                                df_relacionamento, 
                                partition_by=["year", "month", "day"],
                                storage_options=storage_options,
                                mode="append",
                                )
                print(f"Gravado Relacionamento 2 na Trused!{psutil.virtual_memory()._asdict()}")
                
                del df_relacionamento