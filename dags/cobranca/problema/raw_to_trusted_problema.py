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

    ### Coletando dados da camada Raw
    # Conectando com o banco
    conn = connect(
        host=access_params['trino_endpoint'],
        port=access_params['trino_port'],
        user=access_params['trino_user'],
        auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
        http_scheme="https",
    )

    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        return pd.DataFrame(rows, columns=columns)


    minio_raw = Minio(
        access_params['endpoint_url_raw'],
        access_key=access_params['aws_access_key_id_raw'],
        secret_key=access_params['aws_secret_access_key_raw'],
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
        df = pd.concat(ALL_DATA, ignore_index=True)
        print("✅ Dados empilhados com sucesso!")
    else:
        print("⚠️ Nenhum dado foi carregado.")

    
    print('Tratando dados para trusted...')
    print('Quantidade de linhas após empilhamento:')
    print(len(df))

    # Padroniza nome das colunas
    def normalize_column(col):
        col = unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('utf-8')
        col = col.lower().strip().replace(" ", "_")
        col = re.sub(r"[^\w_]", "", col)
        return col

    # Aplica normalização às colunas
    df.columns = [normalize_column(c) for c in df.columns]


    df['cnpj'] = df['cnpj'].str.replace(r'[./-]', '', regex=True)
    df['raiz_cnpj'] = df['cnpj'].str[:8]


    df['problema'] = df['status'].str.strip().str.lower().str.contains(
        r'inadimpl[eê]ncia|em cobran[çc]a|irrecuper[aá]vel', 
        regex=True, na=False
    ).map({True: 'SIM', False: 'NÃO'})


    # Colunas que aparentam ser datas e precisam de conversão
    date_cols = [col for col in df.columns if "data" in col]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

    # Normalizando campos string
    df['safra'] = df['safra'].astype(str)
    df['status'] = df['status'].str.upper()


    df = df[['safra', 'cnpj','raiz_cnpj', 'status', 'problema']].drop_duplicates().reset_index(drop=True)

    # Ano mes e Chave para Power BI
    df["ano_mes"] = pd.to_datetime(df["safra"]).dt.strftime("%Y%m")

    # Priorizar status de Problema SIM e evitar dois tipos de status para cada cnpj em um fechamento especifico
    df_agrupado = df.groupby(['safra', 'ano_mes', 'cnpj', 'raiz_cnpj'], as_index=False).agg({
        'problema': 'max'
    })

    df_agrupado['chave_is_problema'] = df_agrupado['ano_mes'] + df_agrupado['cnpj']

    # Cruzando com base raiz para pegar o status daquele problema
    df_final = pd.merge(df_agrupado, df, on=['problema', 'safra', 'cnpj', 'ano_mes', 'raiz_cnpj'], how='left')

    df_final = df_final.groupby(['safra', 'ano_mes', 'cnpj', 'raiz_cnpj', 'problema', 'chave_is_problema'], as_index=False).agg({
        'status': 'first',
    }).drop_duplicates().reset_index(drop=True)

    '''
    ## Query dados de boletos prorrogados
    query_prorrogados = f"""
                            select
                                distinct
                                last_day_of_month(b.safra_concessao) as safra,
                                date_format(b.safra_concessao, '%Y%m') AS ano_mes,
                                regexp_replace(b.cnpj_sacado, '[./-]', '') as cnpj,
                                substring(regexp_replace(b.cnpj_sacado, '[./-]', ''), 1, 8) as raiz_cnpj,
                                'NÃO' as problema,
                                date_format(b.safra_concessao, '%Y%m') || regexp_replace(b.cnpj_sacado, '[./-]', '') as chave_is_problema,
                                'TÍTULO PRORROGADO' as status
                            from deltalaketrusted.payments.boletos_internos b
                            where status_liquidez = 'PRORROGADO'
                            and try_cast(data_emissao as date) >= date '2025-01-01'
                        """

    df_prorrogados = execute_query(conn, query_prorrogados)

    df_prorrogados['safra'] = pd.to_datetime(df_prorrogados['safra']).dt.strftime("%Y-%m-%d")

    # Filtrar apenas chaves que não estão na acumulada
    df_prorrogados_filtrado = df_prorrogados[
        ~df_prorrogados['chave_is_problema'].isin(df_final['chave_is_problema'])
    ]

    # Inserindo casos de prorrogados na acumulada
    df_final = pd.concat([df_final, df_prorrogados_filtrado], ignore_index=True)
    '''


    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_final['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_final['year'] = now.year
    df_final['month'] = now.month


    df_final = df_final.reset_index(drop=True)

    print(df_final)

    print('Quantidade de linhas para exportar:')
    print(len(df_final))

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
        df_final, 
        partition_by=["year", "month"],
        storage_options=storage_options_trusted,
        mode="overwrite"
    )

    print('Arquivo salvo com sucesso!')