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
from airflow.models import Variable


def cria_tabela_registros_oficiais (access_params=None, **kwargs):

    ### CONECTANDO COM O TRINO
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


    ### QUERY RO

    query_ro = f"""
                select
                    i.boleto_titulo_id,
                    ai.codigo_ro,
                    i.numero_titulo,
                    i.chave_nfe,
                    ti.descricao tipo,
                    CASE
                        WHEN i.status_instrucao_id = 1 AND i.confirmado = false THEN 'EM ANDAMENTO'
                        WHEN i.status_instrucao_id = 2 AND i.confirmado = true THEN 'PROCEDENTE'
                        WHEN i.status_instrucao_id = 5 AND i.confirmado = false THEN 'IMPROCEDENTE'
                        ELSE 'SEM STATUS'
                    END AS status_ro,
                    i.motivo
                FROM postgres.ccred_schema_{Variable.get('STAGE')}_default.instrucao i
                inner join postgres.ccred_schema_{Variable.get('STAGE')}_default.tipo_instrucao ti on ti.id = i.tipo_id
                INNER JOIN postgres.knkt_intr_default.arcelor_instruction ai on ai.id = cast(i.referencia_externa as int)
                    """

    df_ro = execute_query (conn, query_ro)


    # TRATANDO COLUNAS DE TEXTO

    df_ro['tipo'] = df_ro['tipo'].str.upper()

    df_ro['motivo'] = df_ro['motivo'].str.upper()


    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_ro['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_ro['year'], df_ro['month'], df_ro['day'] = now.year, now.month, now.day

    print('Salvando dados na camada trusted...')

    storage_options_trusted = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_TRUSTED = "cobranca"
    FOLDER_DESTINATION_TRUSTED = "registros_oficiais"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}",
        df_ro, 
        partition_by=["year", "month"],
        storage_options=storage_options_trusted,
        mode="overwrite"
    )

    print('Arquivo salvo com sucesso!')