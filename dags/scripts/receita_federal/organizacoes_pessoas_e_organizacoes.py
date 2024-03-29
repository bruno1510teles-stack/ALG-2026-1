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
def transform_receita_to_organizacoes(files_list_estabelecimento, files_list_empresa, access_params):

    # Variaveis Conexão
    BUCKET_SOURCE_RAW = "receita-federal-minio"
    RAW_EMPRESA_FOLDER =  "empresas/"
    RAW_ESTABELECIMENTO_FOLDER = "estabelecimentos/"
    BUCKET_SOURCE_TRUSTED = "pessoas-e-organizacoes"
    TRUSTED_FOLDER = "organizacoes/"

    # Conectando na trusted
    client = Minio(
        access_params['endpoint_url_raw'],
        access_key = access_params['aws_access_key_id_raw'],
        secret_key = access_params['aws_secret_access_key_raw'],
    )


    print('IMPORTANDO EMPRESA')
    # Importando dados de EMPRESA
    
    dtype_empresa = {
            0:'string',
            1:'string',
            4:'string',
            5:'string'
        }


    cols = [0, 1, 4, 5]   
        
    for file_name in files_list_empresa:
        print(f"file_name: {file_name}")
        file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
        df_empresa = pd.read_csv(BytesIO(file.data), sep=';', 
                            encoding='latin1', low_memory=False,  dtype=dtype_empresa, usecols=cols, header=None)


        # Renomeando colunas com o nome padrão da Recita
        colunas_empresa = {0: 'CNPJ BÁSICO',
        1: 'RAZÃO SOCIAL / NOME EMPRESARIAL',
        4: 'CAPITAL SOCIAL DA EMPRESA',
        5: 'PORTE DA EMPRESA'}    
        
        df_empresa = df_empresa.rename(columns=colunas_empresa)
        
        # Substituindo os valores da coluna 'PORTE DA EMPRESA' de acordo com o dicionário
        codigo_tamanho_empresa = {
        '00': 'NÃO INFORMADO',
        '01': 'MICRO EMPRESA',
        '03': 'EMPRESA DE PEQUENO PORTE',
        '05': 'DEMAIS'
            }
        
        df_empresa['PORTE DA EMPRESA'] = df_empresa['PORTE DA EMPRESA'].map(codigo_tamanho_empresa)
        
        # Importando dados de ESTABELECIMENTO
        print('IMPORTANDO ESTABELECIMENTO')
        # Definindo tipo da coluna
        dtype_estabelecimentos = {0:'string', 1:'string', 2:'string', 4:'string', 10:'string'}
        
        
        colunas_estabelecimentos = [0, 1, 2, 4, 10]
        
        for file_name in files_list_estabelecimento:
            print(f"file_name: {file_name}")
            file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
            for df_estabelecimentos in pd.read_csv(BytesIO(file.data), sep=';', 
                                encoding='latin1', low_memory=False, chunksize=2000000, dtype=dtype_estabelecimentos, usecols=colunas_estabelecimentos, header=None):
            

                print(f"Coluna Renomeada! {psutil.virtual_memory()._asdict()}")
                #renomeando colunas conforme padrão da Receita
                colunas_estabelecimentos = {0:'CNPJ BÁSICO',
                    1:'CNPJ ORDEM',
                    2:'CNPJ DV',
                    4:'NOME FANTASIA',
                    10:'DATA DE INÍCIO ATIVIDADE'}

                df_estabelecimentos = df_estabelecimentos.rename(columns=colunas_estabelecimentos)
                
                # CRIANDO A TABELA DE ORGANIZAÇÕES:
                print(f"Merge com Empresa {psutil.virtual_memory()._asdict()}")
                df_organizacoes = df_estabelecimentos.merge(df_empresa, how='left',  on='CNPJ BÁSICO')
                
                df_organizacoes['identificador'] = df_organizacoes['CNPJ BÁSICO'].astype(str).str.cat(
                    [df_estabelecimentos['CNPJ ORDEM'].astype(str), df_estabelecimentos['CNPJ DV'].astype(str)], sep='')

                df_organizacoes = df_organizacoes.drop(columns=['CNPJ BÁSICO', 'CNPJ ORDEM', 'CNPJ DV'])
                
                print(f"Renomeando tabela final! {psutil.virtual_memory()._asdict()}")
                df_organizacoes = df_organizacoes.rename(columns = {
                'NOME FANTASIA': 'nome',
                'DATA DE INÍCIO ATIVIDADE': 'inicio',
                'RAZÃO SOCIAL / NOME EMPRESARIAL': 'razao_social',
                'CAPITAL SOCIAL DA EMPRESA': 'capital',
                'PORTE DA EMPRESA': 'tamanho'
                    })
                
                print(f"ordenando coluna final {psutil.virtual_memory()._asdict()}")
                df_organizacoes = df_organizacoes[['identificador', 'nome', 'razao_social', 'inicio', 'capital', 'tamanho']]
                
                df_organizacoes['idade'] = None
                df_organizacoes['fonte'] = 'RECEITA FEDERAL'
                
                
                #Definindo data de tratamento do arquivo
                now = datetime.now(tz=timezone(timedelta(hours=-3)))
                
                df_organizacoes['year'] = now.year
                df_organizacoes['month'] = now.month
                df_organizacoes['day'] = now.day
                    
                storage_options = {
                    "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
                    "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
                    "AWS_ENDPOINT_URL":f"https://{access_params['endpoint_url_trusted']}",
                    "AWS_REGION": "us-east-1",
                    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
                }
                
                print("Transformando em Pyarrow!")
                # O pandas cria esse index, este codigo serve para remover caso ele crie
                df_organizacoes = pa.Table.from_pandas(df_organizacoes, preserve_index=False)

                print("Gravando na Trused!")
                write_deltalake(f"s3a://{BUCKET_SOURCE_TRUSTED}/{TRUSTED_FOLDER}", 
                                df_organizacoes, 
                                partition_by=["year", "month", "day"],
                                storage_options=storage_options,
                                mode="append",
                                )
                
                print("Gravado na Trused!")