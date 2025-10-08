# Importando Libs
import requests
import pandas as pd
import numpy as np
import base64
from minio import Minio
from io import BytesIO
from requests.auth import HTTPBasicAuth
import json
from airflow.utils.log.logging_mixin import LoggingMixin
from deltalake import write_deltalake, DeltaTable
from datetime import datetime, timezone, timedelta
import unicodedata
import string
from trino.dbapi import connect
from trino.auth import BasicAuthentication


def processa_tabela_prorrogados (access_params=None, **kwargs):

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
    

    ### QUERY PRORROGAÇÕES

    query = f"""
            select
                distinct
                last_day_of_month(b.safra_concessao) as safra,
                date_format(b.safra_concessao, '%Y%m') AS ano_mes,
                b.cnpj_sacado as cnpj,
                regexp_replace(b.cnpj_sacado, '[./-]', '') as cnpj_formatado,
                substring(regexp_replace(b.cnpj_sacado, '[./-]', ''), 1, 8) as raiz_cnpj,
                lpad(b.numero_titulo, 10, '0') as numero_titulo,
                b.valor_face,
                'NÃO' as problema,
                b.cnpj_sacado || '-' || lpad(b.numero_titulo, 10, '0') as chave,
                'TÍTULO PRORROGADO' as status
            from deltalaketrusted.payments.boletos_internos b
            where status_liquidez = 'PRORROGADO'
            and status_titulo = 'A VENCER'
            """

    df_prorrogados = execute_query (conn, query)


    # Colunas de data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_prorrogados['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_prorrogados['year'], df_prorrogados['month'], df_prorrogados['day'] = now.year, now.month, now.day
    print("Tratamento dos dados concluído")



    # Configurações para acesso ao MinIO
    logger = LoggingMixin().log 

    try:
        logger.info("Iniciando salvamento das informações")
            
        storage_options = {
            "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
            "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
            "AWS_ENDPOINT_URL":f"https://{access_params['endpoint_url_trusted']}",
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }


        # Definindo o caminho e salvando no MinIO
        BUCKET_SOURCE_TRUSTED = "cobranca"
        FOLDER_DESTINATION_TRUSTED = "prorrogados"

        write_deltalake(
            f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}", 
            df_prorrogados, 
            partition_by=["year", "month", "day"],
            storage_options=storage_options,
            mode="overwrite"
            #overwrite_schema=True
    )
        logger.info("Salvamento concluído com sucesso.")
        
    except Exception as e:
        logger.error(f"Erro ao salvar as informações: {str(e)}")