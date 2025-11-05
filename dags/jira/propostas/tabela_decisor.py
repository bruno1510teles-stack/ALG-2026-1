# Importando Bibliotecas
import requests
import pandas as pd
import numpy as np
import base64
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from minio.error import S3Error
from io import BytesIO
from requests.auth import HTTPBasicAuth
import json
from deltalake import write_deltalake, DeltaTable
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, time, timedelta, timezone

def decisor_trusted (access_params=None, **kwargs):


    # Conectando ao Trino para Leitura
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
    
    # Definindo a consulta
    query_propostas_aprovadas = """
            WITH primeira_aprovacao AS (
                SELECT
                    raiz_cnpj,
                    categoria_decisor,
                    data_resolvido,
                    ROW_NUMBER() OVER (
                        PARTITION BY raiz_cnpj
                        ORDER BY data_resolvido ASC
                    ) AS rn
                FROM deltalaketrusted.jira.propostas
                WHERE decisao = 'APROVADO'
            )

            SELECT 
                raiz_cnpj,
                categoria_decisor,
                data_resolvido
            FROM primeira_aprovacao
            WHERE rn = 1

            """

    # Verifica se a conexão foi bem-sucedida antes de executar a consulta
    if conn is not None:
        df_aprovadas = execute_query(conn, query_propostas_aprovadas)
    else:
        df_aprovadas = None
        print("A consulta não foi executada porque a conexão com o Trino falhou.")


    # Inspecionando colunas do DataFrame
    print("Colunas carregadas e disponíveis no DataFrame:")
    print(df_aprovadas.columns.tolist())


    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_aprovadas['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_aprovadas['year'], df_aprovadas['month'], df_aprovadas['day'] = now.year, now.month, now.day
    print("Tratamento dos dados concluído")


    # Exportando dados para a camada Trusted
    # # Conectando na Trusted        
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }


    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_TRUSTED = "jira"
    FOLDER_DESTINATION_TRUSTED = "categoria_decisor"
    
    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}", 
        df_aprovadas, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )