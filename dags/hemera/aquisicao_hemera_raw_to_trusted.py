# Importando Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from deltalake import write_deltalake, DeltaTable
import os
from concurrent.futures import ThreadPoolExecutor
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timezone, timedelta
import re

def hemera_raw_to_trusted(access_params=None, **kwargs):

    client = Minio(
                access_params['endpoint_url_raw'],
                access_key= access_params['aws_access_key_id_raw'],
                secret_key=access_params['aws_secret_access_key_raw'],
                secure=True
            )

    # Definindo bucket e caminho do arquivo
    BUCKET_SOURCE_RAW = "hemera"
    FOLDER_DESTINATION_RAW = 'aquisicao/year=2025/month=03'

    # Listando os arquivos no diretório
    objects = list(client.list_objects(BUCKET_SOURCE_RAW, prefix=FOLDER_DESTINATION_RAW, recursive=True))

    # Função para extrair a data do nome do arquivo
    def extract_date_from_filename(filename):
        # Expressão regular para capturar o padrão de data (dd.mm.yy)
        date_match = re.search(r'(\d{2}\.\d{2}\.\d{2})', filename)
        if date_match:
            # Converter a string de data para o formato datetime
            return datetime.strptime(date_match.group(1), '%d.%m.%y').date()
        return None

    # Função para ler o arquivo do MinIO e retornar um DataFrame
    def read_file_from_minio(obj):
        file_path = obj.object_name
        print(f"Lendo arquivo: {file_path}")
        
        # Lendo o arquivo do MinIO
        response = client.get_object(BUCKET_SOURCE_RAW, file_path)
        file_data = BytesIO(response.read())
        
        # Assumindo que os arquivos são Excel
        df = pd.read_excel(file_data)

        # Extraindo a data do nome do arquivo
        data_arquivo = extract_date_from_filename(file_path)

        # Extraindo o mês do caminho do arquivo
        match = re.search(r'/month=(\d{2})/', file_path)
        if match:
            mes = match.group(1)
            # Criando a coluna 'data_ref'
            df['data_ref'] = f"2025-{mes}"

        # Adicionar a coluna 'data_arquivo' ao DataFrame
        df['data_arquivo'] = data_arquivo

        return df

    # Usar ThreadPoolExecutor para ler arquivos em paralelo
    with ThreadPoolExecutor() as executor:
        dfs = list(executor.map(read_file_from_minio, objects))

    # Concatenar todos os DataFrames
    df_consolidado = pd.concat(dfs, ignore_index=True)


    # Tratando base
    # Função para converter os números do formato Excel para datas
    def converter_data_excel(data):
        # Verifica se a entrada é um número (formato Excel)
        if isinstance(data, (int, float)):
            # Converte do formato Excel para a data correta
            return pd.to_datetime(data, origin='1899-12-30', unit='D')
        else:
            # Retorna a data original, caso já esteja em formato datetime
            return pd.to_datetime(data, errors='coerce')

    # Aplicar a função à coluna 'DataPosicao'
    df_consolidado['DataVencimento'] = df_consolidado['DataVencimento'].apply(converter_data_excel)
    df_consolidado['DataEmissao'] = df_consolidado['DataEmissao'].apply(converter_data_excel)
    df_consolidado['DataAquisicao'] = df_consolidado['DataAquisicao'].apply(converter_data_excel)


    # Renomear a coluna 'ultima_data' para 'data_fechamento'
    data_fechamento = df_consolidado['data_arquivo'] + pd.offsets.MonthEnd(0)
    df_consolidado['data_fechamento'] = data_fechamento


    # Reorganizar as colunas para que 'data_fechamento' seja a primeira
    colunas = ['data_fechamento'] + [col for col in df_consolidado.columns if col != 'data_fechamento']
    df_consolidado = df_consolidado[colunas]

    # Converter a coluna de datas para strings formatadas
    # Tratando Dados
    def converter_para_datetime(df, colunas, formato='%Y-%m-%d'):
        for coluna in colunas:
            try:
                # Tenta converter a coluna para datetime, ignorando erros
                df[coluna] = pd.to_datetime(df[coluna], format=formato, errors='coerce')
                
                # Verifica se a coluna foi convertida corretamente
                if df[coluna].isnull().any():
                    print(f"Alguns valores na coluna '{coluna}' não puderam ser convertidos para datetime.")
                
                # Acessa apenas a parte da data (sem a hora) se for um datetime válido
                df[coluna] = df[coluna].dt.date
            except Exception as e:
                print(f"Erro ao converter a coluna '{coluna}': {e}")
        
        return df
    
    colunas_para_converter_datetime = ['DataVencimento', 'DataEmissao', 'DataAquisicao', 'data_fechamento']
    df_consolidado = converter_para_datetime(df_consolidado, colunas_para_converter_datetime)

    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_consolidado['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_consolidado['year'] = df_consolidado['data_fechamento'].apply(lambda x: x.year if pd.notnull(x) else None)
    df_consolidado['month'] = df_consolidado['data_fechamento'].apply(lambda x: x.month if pd.notnull(x) else None)

    df_consolidado['NumeroTitulo'] = df_consolidado['NumeroTitulo'].astype(str)

    ## Filtrando apenas colunas necessárias e renomando-as
    df_consolidado.rename(columns={
        'ValorAquisicao': 'valor_aquisicao',
        'IdTituloVx': 'id_titulo',
    }, inplace=True)

    colunas_finais = ['data_fechamento', 'data_ref', 'id_titulo','valor_aquisicao', 'year', 'month', 'atualizado_em']
    # Aplicando a função para renomear as colunas no DataFrame
    df_consolidado = df_consolidado[colunas_finais]

    # Padronizando Outputs
    # Formatar a coluna 'data_fechamento' para o formato YYYY/MM/D
    df_consolidado['data_fechamento'] = pd.to_datetime(df_consolidado['data_fechamento']).dt.strftime('%Y-%m-%d')

    # Garantir que 'IdTituloVx' tenha exatamente 10 caracteres, completando com zeros à esquerda se necessário
    df_consolidado['id_titulo'] = df_consolidado['id_titulo'].astype(str).str.zfill(10)

    # Formatar 'valor_aquisicao' para ter 2 casas decimais
    df_consolidado['valor_aquisicao'] = pd.to_numeric(df_consolidado['valor_aquisicao'], errors='coerce').round(2)

    # Garantir que 'year' e 'month' sejam strings
    df_consolidado['year'] = df_consolidado['year'].astype(str)
    df_consolidado['month'] = df_consolidado['month'].astype(str)

    # Formatar 'atualizado_em' para data completa com hora
    df_consolidado['atualizado_em'] = pd.to_datetime(df_consolidado['atualizado_em']).dt.strftime('%Y-%m-%d %H:%M:%S')

    # Verificando o resultado
    print(df_consolidado.head())

    # Exportando dados para a camada Trusted
    # # Conectando na Trusted
        
    print('Salvando Arquivo')   

    storage_options_trusted = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_TRUSTED = "hemera"
    FOLDER_DESTINATION_TRUSTED = "aquisicao"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}",
        df_consolidado, 
        partition_by=["year", "month"],
        storage_options=storage_options_trusted,
        mode="append"
    )

    print('Arquivo salvo com sucesso!')