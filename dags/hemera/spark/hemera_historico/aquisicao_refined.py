# Importing Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import numpy as np


def aquisicao_trusted_to_refined (access_params=None, **kwargs):

    # Coletando dados da camada Trusted
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

    query_aquisicao = f"""
                        select *
                        from deltalaketrusted.hemera_trusted.aquisicao_consolidado
                    """

    df = execute_query(conn, query_aquisicao)

    print(f"Quantidade de linhas no DataFrame 'aquisicao' nulas:")
    df.isnull().sum()


    colunas_finais = ['data_fechamento','id_titulo','valor_aquisicao', 'year', 'month', 'atualizado_em']
    # Aplicando a função para renomear as colunas no DataFrame
    df_consolidado = df[colunas_finais].copy()


    df_consolidado['data_fechamento'] = pd.to_datetime(df_consolidado['data_fechamento'], errors='coerce')
    df_consolidado['data_fechamento'] = df_consolidado['data_fechamento'].dt.date
    df_consolidado['id_titulo'] = df_consolidado['id_titulo'].astype(str).str.zfill(10)
    df_consolidado['valor_aquisicao'] = pd.to_numeric(df_consolidado['valor_aquisicao'], errors='coerce').round(2)
    df_consolidado['year'] = df_consolidado['year'].astype(str)
    df_consolidado['month'] = df_consolidado['month'].astype(str)
    df_consolidado['atualizado_em'] = pd.to_datetime(df_consolidado['atualizado_em']).dt.strftime('%Y-%m-%d %H:%M:%S')


    print(df_consolidado)


    print('Salvando dados na Refined...')

    storage_options_refined = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_refined']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }



    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_REFINED = "hemera-refined"
    FOLDER_DESTINATION_REFINED = "aquisicao/delta"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}",
        df_consolidado,
        storage_options=storage_options_refined,
        mode="overwrite"
    )

    print('Arquivo salvo com sucesso!')