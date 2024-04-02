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
def transform_receita_to_situacao_cadastral(files_list_estabelecimento, access_params):

    # Variaveis Conexão
    BUCKET_SOURCE_RAW = "receita-federal"
    RAW_ESTABELECIMENTO_FOLDER = "estabelecimentos/"
    BUCKET_SOURCE_TRUSTED = "pessoas-e-organizacoes"
    TRUSTED_FOLDER = "situacao-cadastral/"

    # Conectando na trusted
    client = Minio(
        access_params['endpoint_url_raw'],
        access_key = access_params['aws_access_key_id_raw'],
        secret_key = access_params['aws_secret_access_key_raw'],
    )
    
    # Importando dados de ESTABELECIMENTO
    print('IMPORTANDO ESTABELECIMENTO')

    #definindo colunas a serem utilizadas
    colunas_estabelecimentos = [0, 1, 2, 5, 6]

    #definindo tipo das colunas para importacao
    dtypes_estabelecimentos = {0:'string', 1:'string', 2:'string', 5:'string', 6:'string'}
    
    tentativas = 3
    tentativa_atual = 0
    sucesso = False

    #Dentro do loop de empresa é feito um loop de estabelecimentos no qual, cada arquivo de estabelecimento vai ser divido em chunks e porcessado por partes
    for file_name in files_list_estabelecimento:
        print(f"file_name: {file_name}")
        while tentativa_atual < tentativas and not sucesso:
            try:
                file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
                for df in pd.read_csv(BytesIO(file.data), sep=';', 
                                    encoding='latin1', low_memory=False, chunksize=6000000, dtype=dtypes_estabelecimentos, usecols=colunas_estabelecimentos, header=None):
                
                    #definindo nome das colunas para tratamento inicial
                    nome_colunas_estabelecimentos = {0:'CNPJ BÁSICO',
                        1:'CNPJ ORDEM',
                        2:'CNPJ DV',
                        5:'status',
                        6:'data'}

                    df = df.rename(columns=nome_colunas_estabelecimentos)

                    # Substituindo os valores da coluna 'status da empresa' de acordo com o dicionário
                    codigo_status_empresa = {
                        '01': 'NULA',
                        '02': 'ATIVA',
                        '03': 'SUSPENSA',
                        '04': 'INAPTA',
                        '08': 'BAIXADA'}

                    df['status'] = df['status'].map(codigo_status_empresa)

                    # Substituindo os valores que tem 0 como data de alteração de situação cadastral para nulos
                    df.loc[df['data'] == '0','data'] = None

                    # Transformando colunas de data da situação cadastral no tipo data
                    df['data'] = pd.to_datetime(df['data'], format='%Y%m%d').dt.date

                    # Juntando os diferentes campos de cnpj em uma coluna de documento unico
                    df['identificador'] = df['CNPJ BÁSICO'].astype(str).str.cat(
                        [df['CNPJ ORDEM'].astype(str), df['CNPJ DV'].astype(str)], sep='')

                    df = df.drop(columns=['CNPJ BÁSICO', 'CNPJ ORDEM', 'CNPJ DV'])

                    #alterando ordem das colunas
                    df = df[['identificador', 'status', 'data']]
                    
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
                    sucesso = True
                    
            except Exception as e:
                tentativa_atual += 1
                print(f"Tentativa {tentativa_atual} falhou:", str(e))
                sucesso = False    
                
            if not sucesso and tentativa_atual == tentativas:
                raise RuntimeError("Falha após 3 tentativas. Importação não foi bem-sucedida.")
            else:
                sucesso = False  # resetar o sucesso para próxima iteração
                tentativa_atual = 0