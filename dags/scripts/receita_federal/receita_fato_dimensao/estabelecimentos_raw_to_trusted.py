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
def estabelecimento_to_trusted(files_list_estabelecimento, access_params):

    # Variaveis Conexão
    BUCKET_SOURCE_RAW = "receita-federal"
    BUCKET_SOURCE_TRUSTED = "receita-federal"
    TRUSTED_FOLDER = "estabelecimentos/"

    # Conectando na trusted
    client = Minio(
        access_params['endpoint_url_raw'],
        access_key = access_params['aws_access_key_id_raw'],
        secret_key = access_params['aws_secret_access_key_raw'],
    )
    header_inicial = ['CNPJ BÁSICO',
        'CNPJ ORDEM',
        'CNPJ DV',
        'IDENTIFICADOR MATRIZ/FILIAL',
        'NOME FANTASIA',
        'SITUAÇÃO CADASTRAL',
        'DATA SITUAÇÃO CADASTRAL',
        'MOTIVO SITUAÇÃO CADASTRAL',
        'NOME DA CIDADE NO EXTERIOR', 
        'PAIS',
        'DATA DE INÍCIO ATIVIDADE',
        'CNAE FISCAL PRINCIPAL',
        'CNAE FISCAL SECUNDÁRIA',
        'TIPO DE LOGRADOURO',
        'LOGRADOURO',
        'NÚMERO',
        'COMPLEMENTO',
        'BAIRRO',
        'CEP',
        'UF',
        'MUNICÍPIO',
        'DDD 1',
        'TELEFONE 1',
        'DDD 2',
        'TELEFONE 2',
        'DDD DO FAX',
        'FAX',
        'CORREIO ELETRÔNICO',
        'SITUAÇÃO ESPECIAL',
        'DATA DA SITUAÇÃO ESPECIAL']
    
    # Importando dados de ESTABELECIMENTOS
    print(f'IMPORTANDO ESTABELECIMENTOS {psutil.virtual_memory()._asdict()}')

    for file_name in files_list_estabelecimento:
        print(f"file_name: {file_name} {psutil.virtual_memory()._asdict()}")
        file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
        df = pd.read_csv(BytesIO(file.data), sep=';', encoding = 'latin1', header = None, dtype=str)
        print(f'df importado {psutil.virtual_memory()._asdict()}')
        
        df.columns = header_inicial

        df['documento_sem_formatacao'] = df['CNPJ BÁSICO'] + df['CNPJ ORDEM'] + df['CNPJ DV']
        df['documento_formatado'] = df['documento_sem_formatacao'].str[0:2] + '.' + df['documento_sem_formatacao'].str[2:5] + '.' + df['documento_sem_formatacao'].str[5:8] + '/' + df['documento_sem_formatacao'].str[8:12] + '-' + df['documento_sem_formatacao'].str[12:14]
        df['logradouro'] = df['TIPO DE LOGRADOURO'] + ' ' + df['LOGRADOURO']
        df['telefone_1'] = '(' + df['DDD 1'] + ')'  + df['TELEFONE 1']
        df['telefone_2'] = '(' + df['DDD 2'] + ')'  + df['TELEFONE 2']
        
        df = df.drop(columns = {'CNPJ ORDEM', 'CNPJ DV', 'TIPO DE LOGRADOURO', 'LOGRADOURO', 'DDD 1', 'TELEFONE 1', 'DDD 2', 'TELEFONE 2', 'DDD DO FAX', 'FAX'})

        renomeando = {'IDENTIFICADOR MATRIZ/FILIAL': 'identificador_matriz/filial',
            'CNPJ BÁSICO': 'cnpj_raiz', 
            'NOME FANTASIA': 'nome_fantasia',
            'SITUAÇÃO CADASTRAL': 'situacao_cadastral',
            'DATA SITUAÇÃO CADASTRAL': 'data_situacao_cadastral',
            'MOTIVO SITUAÇÃO CADASTRAL': 'motivo_situacao_cadastral',
            'NOME DA CIDADE NO EXTERIOR': 'nome_cidade_exterior', 
            'PAIS': 'pais',
            'DATA DE INÍCIO ATIVIDADE': 'data_inicio_atividade',
            'CNAE FISCAL PRINCIPAL': 'cnae_principal',
            'CNAE FISCAL SECUNDÁRIA': 'cnae_secundaria',
            'NÚMERO': 'numero',
            'COMPLEMENTO': 'complemento',
            'BAIRRO': 'bairro',
            'CEP': 'cep',
            'UF': 'uf',
            'MUNICÍPIO': 'municipio',
            'CORREIO ELETRÔNICO': 'email',
            'SITUAÇÃO ESPECIAL': 'situacao_especial',
            'DATA DA SITUAÇÃO ESPECIAL': 'data_sitaucao_especial'}
        
        df = df.rename(columns = renomeando)
        
        colunas_ordem = [
        'cnpj_raiz',
        'documento_sem_formatacao',
        'documento_formatado',
        'identificador_matriz/filial',
        'nome_fantasia',
        'situacao_cadastral',
        'data_situacao_cadastral',
        'motivo_situacao_cadastral',
        'nome_cidade_exterior', 
        'pais',
        'data_inicio_atividade',
        'cnae_principal',
        'cnae_secundaria',
        'logradouro',
        'numero',
        'complemento',
        'bairro',
        'cep',
        'uf',
        'municipio',
        'telefone_1',
        'telefone_2',
        'email',
        'situacao_especial',
        'data_sitaucao_especial']
        
        df['data_situacao_cadastral'] = df['data_situacao_cadastral'].str[0:4] + '-' + df['data_situacao_cadastral'].str[4:6] + '-' + df['data_situacao_cadastral'].str[6:8]
        df['data_inicio_atividade'] = df['data_inicio_atividade'].str[0:4] + '-' + df['data_inicio_atividade'].str[4:6] + '-' + df['data_inicio_atividade'].str[6:8]
        df['data_sitaucao_especial'] = df['data_sitaucao_especial'].str[0:4] + '-' + df['data_sitaucao_especial'].str[4:6] + '-' + df['data_sitaucao_especial'].str[6:8]

        df = df[colunas_ordem]

        df['mes_ref'] = file_name[-13:-11]
        df['ano_ref'] = str(datetime.now().year)[0:3] + file_name[-14:-13]

        (f"Criando colunas de particionamento {psutil.virtual_memory()._asdict()}")
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

        print(f"Gravado na Trused! {psutil.virtual_memory()._asdict()}")
