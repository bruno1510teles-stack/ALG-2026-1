# Importing Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake
import os
import tempfile
import msoffcrypto
import re
import numpy as np
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import unicodedata


def trata_safras_problema (access_params=None, **kwargs):

    minio_raw = Minio(
        "api-raw.alpe.com.br",
        access_key = 'B7q0avvSIpSdyGPXWnEC',
        secret_key = 'PhMhRQSQ6YJU8fn2qKhDLM017cQPrlCz1YbM8IwU'
    )

    # Connection validation
    try:
        # Try to list the buckets
        buckets = minio_raw.list_buckets()
        
        # If the connection was successful, print the buckests
        print("Conexão bem-sucedida. Lista de buckets disponíveis:")
        for bucket in buckets:
            print(bucket.name)
        
    except Exception as e:
            # If the connection was failed, print the error message
            print(f"Erro ao conectar ao MinIO: {e}")

    

    BUCKET_NAME = "cobranca"
    PREFIX = "relatorio_acompanhamento_cobranca/"
    ALL_DATA = []

    objects = minio_raw.list_objects(BUCKET_NAME, prefix=PREFIX, recursive=True)

    for obj in objects:
        file_key = obj.object_name
        if file_key.endswith(".xlsx"):
            print(f"📥 Lendo Excel: {file_key}")

            try:
                response = minio_raw.get_object(BUCKET_NAME, file_key)
                excel_data = BytesIO(response.read())

                # Lê o Excel como DataFrame
                df = pd.read_excel(excel_data, dtype=str)

                # Remove linhas totalmente vazias
                df.dropna(how="all", inplace=True)

                # Extrai a data da safra do nome do arquivo
                match = re.search(r"(\d{2}-\d{2}-\d{4})", file_key)
                safra = pd.NaT
                if match:
                    safra = datetime.strptime(match.group(1), "%d-%m-%Y")

                df["safra"] = safra
                df["source_file"] = file_key

                ALL_DATA.append(df)

            except Exception as e:
                print(f"⚠️ Erro ao ler {file_key}: {e}")

    # Empilha todos os DataFrames
    if ALL_DATA:
        final_df = pd.concat(ALL_DATA, ignore_index=True)
        print("✅ Dados empilhados com sucesso!")
    else:
        print("⚠️ Nenhum dado foi carregado.")

    final_df = final_df.drop(columns=["source_file"])

    # Tratando dados para trusted

    print(final_df)

    # Padroniza nome das colunas
    def normalize_column(col):
        col = unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('utf-8')
        col = col.lower().strip().replace(" ", "_")
        col = re.sub(r"[^\w_]", "", col)
        return col

    # Aplica normalização às colunas
    final_df.columns = [normalize_column(c) for c in final_df.columns]


    final_df['cnpj'] = final_df['cnpj'].str.replace(r'[./-]', '', regex=True)
    final_df['raiz_cnpj'] = final_df['cnpj'].str[:8]


    final_df['problema'] = final_df['status'].str.strip().str.lower().isin([
        'risco inadimplência',
        'em cobrança'
    ]).map({True: 'SIM', False: 'NÃO'})


    # Colunas que aparentam ser datas e precisam de conversão
    date_cols = [col for col in final_df.columns if "data" in col]
    for col in date_cols:
        final_df[col] = pd.to_datetime(final_df[col], errors="coerce", dayfirst=True)


    # safra em formato date
    final_df['safra'] = pd.to_datetime(final_df['safra'], errors='coerce').dt.date

    # Normalizando campos string
    final_df['resumo'] = final_df['resumo'].str.upper()
    final_df['status'] = final_df['status'].str.upper()

    final_df = final_df[['safra', 'cnpj','raiz_cnpj', 'problema']]

    final_df = final_df.drop_duplicates().reset_index(drop=True)

    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    final_df['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    final_df['year'] = now.year
    final_df['month'] = now.month

    final_df['safra'] = pd.to_datetime(final_df['safra'], errors='coerce').dt.date

    print("✅ Dados tratados")


    print('Salvando dados na Trusted...')


    storage_options_trusted = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_TRUSTED = "cobranca"
    FOLDER_DESTINATION_TRUSTED = "relatorio_acompanhamento_cobranca"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}",
        final_df, 
        partition_by=["year", "month"],
        storage_options=storage_options_trusted,
        mode="overwrite"
    )

    print('Arquivo salvo com sucesso!')