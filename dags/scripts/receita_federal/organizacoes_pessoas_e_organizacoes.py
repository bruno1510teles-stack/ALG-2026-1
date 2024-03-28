# Carregando libs
import pandas as pd
import pyarrow as pa
import copy
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
import os
from deltalake import write_deltalake, DeltaTable


# Criando conexão
def transform_receita_to_organizacoes(files_list_estabelecimento, files_list_empresa, access_params):

    # Variaveis Conexão
    BUCKET_SOURCE_RAW = "receita-federal-minio"
    RAW_EMPRESA_FOLDER =  "empresas/"
    RAW_ESTABELECIMENTO_FOLDER = "estabelecimentos/"
    BUCKET_SOURCE_REFINED = "pessoas-e-organizacoes"
    REFINED_FOLDER = "organizacoes/"

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
            2:'string',
            3:'string',
            4:'string',
            5:'string',
            6:'string',
        }
        
    dfs = []

    for file_name in files_list_empresa:
        print(f"file_name: {file_name}")
        file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
        df_empresa_raw_temp = pd.read_csv(BytesIO(file.data), sep = ';', encoding = 'latin1', header=None, engine='c', dtype=dtype_empresa)
        dfs.append(df_empresa_raw_temp)
    # Consolidando    
    df_empresa = pd.concat(dfs, ignore_index=True)
    
    # Renomeando colunas com o nome padrão da Recita
    colunas_empresa = {0: 'CNPJ BÁSICO',
    1: 'RAZÃO SOCIAL / NOME EMPRESARIAL',
    2: 'NATUREZA JURÍDICA',
    3: 'QUALIFICAÇÃO DO RESPONSÁVEL',
    4: 'CAPITAL SOCIAL DA EMPRESA',
    5: 'PORTE DA EMPRESA',
    6: 'ENTE FEDERATIVO RESPONSÁVEL'}
    
    df_empresa = df_empresa.rename(columns=colunas_empresa)
    
    #PEGANDO APENAS COLUNAS NECESSÁRIAS
    df_empresa = df_empresa[['CNPJ BÁSICO', 'RAZÃO SOCIAL / NOME EMPRESARIAL', 'CAPITAL SOCIAL DA EMPRESA', 'PORTE DA EMPRESA']]
    
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
    dtype_estabelecimentos = {0:'string', 1:'string', 2:'string', 3:'string', 4:'string', 
                              5:'string', 6:'string', 7:'string', 8:'string', 9:'string', 
                              10:'string', 11:'string', 12:'string', 13:'string', 14:'string',
                              15:'string', 16:'string', 17:'string', 18:'string', 19:'string',
                              20:'string', 21:'string', 22:'string', 23:'string', 24:'string',
                              25:'string', 26:'string', 27:'string', 28:'string', 29:'string'}
    
    
    for file_name in files_list_estabelecimento:
        print(f"file_name: {file_name}")
        file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
        df_estabelecimentos = pd.read_csv(BytesIO(file.data), sep = ';', encoding = 'latin1', 
                                          header=None, engine='c', dtype=dtype_estabelecimentos, encoding_errors='ignore'
                                          )
    
        # Selecionando apenas colunas necessárias
        df_estabelecimentos = df_estabelecimentos[[0,  1,  2,  4,  5,  6, 10, 11, 12, 13, 14, 15, 16, 17,
        18, 19, 20, 21, 22, 23, 24, 27]]
        
        #renomeando colunas conforme padrão da Receita
        colunas_estabelecimentos = {0:'CNPJ BÁSICO',
            1:'CNPJ ORDEM',
            2:'CNPJ DV',
            3:'IDENTIFICADOR MATRIZ/FILIAL',
            4:'NOME FANTASIA',
            5:'SITUAÇÃO CADASTRAL',
            6:'DATA SITUAÇÃO CADASTRAL',
            7:'MOTIVO SITUAÇÃO CADASTRAL',
            8:'NOME DA CIDADE NO EXTERIOR',
            9:'PAIS',
            10:'DATA DE INÍCIO ATIVIDADE',
            11:'CNAE FISCAL PRINCIPAL',
            12:'CNAE FISCAL SECUNDÁRIA',
            13:'TIPO DE LOGRADOURO',
            14:'LOGRADOURO',
            15:'NÚMERO',
            16:'COMPLEMENTO',
            17:'BAIRRO',
            18:'CEP',
            19:'UF',
            20:'MUNICÍPIO',
            21:'DDD 1',
            22:'TELEFONE 1',
            23:'DDD 2',
            24:'TELEFONE 2',
            25:'DDD DO FAX',
            26:'FAX',
            27:'CORREIO ELETRÔNICO',
            28:'SITUAÇÃO ESPECIAL',
            29:'DATA DA SITUAÇÃO ESPECIAL'}

        df_estabelecimentos = df_estabelecimentos.rename(columns=colunas_estabelecimentos)
        
        
        # CRIANDO A TABELA DE ORGANIZAÇÕES:
        df_estabelecimentos_organizacoes = df_estabelecimentos[['CNPJ BÁSICO', 'CNPJ ORDEM', 'CNPJ DV', 'NOME FANTASIA', 'DATA DE INÍCIO ATIVIDADE']]
        
        df_organizacoes = df_estabelecimentos_organizacoes.merge(df_empresa, how='left',  on='CNPJ BÁSICO')
        
        df_organizacoes['identificador'] = df_organizacoes['CNPJ BÁSICO'].astype(str).str.cat(
            [df_estabelecimentos['CNPJ ORDEM'].astype(str), df_estabelecimentos['CNPJ DV'].astype(str)], sep='')

        df_organizacoes = df_organizacoes.drop(columns=['CNPJ BÁSICO', 'CNPJ ORDEM', 'CNPJ DV'])
        
        df_organizacoes = df_organizacoes.rename(columns = {
        'NOME FANTASIA': 'nome',
        'DATA DE INÍCIO ATIVIDADE': 'inicio',
        'RAZÃO SOCIAL / NOME EMPRESARIAL': 'razao_social',
        'CAPITAL SOCIAL DA EMPRESA': 'capital',
        'PORTE DA EMPRESA': 'tamanho'
            })
        
        df_organizacoes = df_organizacoes[['identificador', 'nome', 'razao_social', 'inicio', 'capital', 'tamanho']]
        
        df_organizacoes['idade'] = None
        df_organizacoes['fonte'] = 'RECEITA FEDERAL'
        
        
        #Definindo data de tratamento do arquivo
        now = datetime.now(tz=timezone(timedelta(hours=-3)))
        
        df_organizacoes['year'] = now.year
        df_organizacoes['month'] = now.month
        df_organizacoes['day'] = now.day
            
        storage_options = {
            "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
            "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
            "AWS_ENDPOINT_URL":f"https://{access_params['endpoint_url_refined']}",
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }
        
        # O pandas cria esse index, este codigo serve para remover caso ele crie
        df_organizacoes = pa.Table.from_pandas(df_organizacoes, preserve_index=False)

        write_deltalake(f"s3a://{BUCKET_SOURCE_REFINED}/{REFINED_FOLDER}", 
                        df_organizacoes, 
                        partition_by=["year", "month", "day"],
                        storage_options=storage_options,
                        mode="append",
                        )