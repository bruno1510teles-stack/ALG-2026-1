# Carregando libs
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from deltalake import write_deltalake
from datetime import datetime, timezone, timedelta
import numpy as np
import re
import logging
from airflow.utils.log.logging_mixin import LoggingMixin
from io import BytesIO
import re


def pontualidade_pagamento (access_params=None,  **kwargs):

    # Carregando base de pagamento do Trino
    conn = connect(
        host='trino.alpe.com.br',
        port='443',
        user='trinodados',
        auth=BasicAuthentication('trinodados', 'hosgzPvuhyXkP<j}RyT+'),
        http_scheme="https",
    )

    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        return pd.DataFrame(rows, columns=columns)

    query_pagamento =  f"""
                        select
                            *
                        from deltalaketrusted.payments.pagamento_externo_arcelor
                    """

    pagamento = execute_query(conn, query_pagamento)


    # Filtrando colunas que vamos usar
    colunas = ['raiz_cnpj', 'vlr_recebido_202404', 'vlr_recebido_202405', 'vlr_recebido_202406', 'vlr_inad_corrente_202404',
            'vlr_inad_corrente_202405', 'vlr_inad_corrente_202406', 'vlr_receb_atrasado_202404', 'vlr_receb_atrasado_202405',
            'vlr_receb_atrasado_202406']

    pagamento = pagamento[colunas]

    # Agrupando por CNPJ e somando valores (pode acontecer de um mesmo CNPJ ter valores diferentes em cada unidade)
    pagamento = pagamento.groupby('raiz_cnpj').sum().reset_index()


    # Criando colunas calculadas para tabela final

    pagamento['recebido_total'] = (
        pagamento['vlr_recebido_202404'] + 
        pagamento['vlr_recebido_202405'] + 
        pagamento['vlr_recebido_202406']
    )

    pagamento['inad_corrente_total'] = (
        pagamento['vlr_inad_corrente_202404'] + 
        pagamento['vlr_inad_corrente_202405'] + 
        pagamento['vlr_inad_corrente_202406']
    )

    pagamento['recebido_atrasado_total'] = (
        pagamento['vlr_receb_atrasado_202404'] + 
        pagamento['vlr_receb_atrasado_202405'] + 
        pagamento['vlr_receb_atrasado_202406']
    )

    pagamento['pontualidade'] = (    
        pagamento['recebido_total'] /
        (
            pagamento['recebido_total'] + 
            pagamento['inad_corrente_total'] + 
            pagamento['recebido_atrasado_total']
        )
    ) * 100

    # Filtrando 
    pagamento = pagamento[pagamento['recebido_total'] > 0]


    # Filtrando colunas para base final
    colunas = ['raiz_cnpj', 'recebido_total', 'inad_corrente_total', 'recebido_atrasado_total', 'pontualidade']

    pagamento_final = pagamento[colunas]

    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    pagamento_final['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    pagamento_final['year'], pagamento_final['month'], pagamento_final['day'] = now.year, now.month, now.day

    pagamento_final = pagamento_final.reset_index(drop=True)


    print('Exportando base para Refined...')

    # Exportando dados para a camada Refined
    # # Conectando na Trusted
    logger = LoggingMixin().log 

    try:
        logger.info("Iniciando salvamento das informações")
        
        storage_options = {
            "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
            "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
            "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_refined']}",
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
        }

        # Definindo o caminho e salvando no MinIO
        BUCKET_SOURCE_REFINED = "motor"
        FOLDER_DESTINATION_REFINED = "pontualidade"

        write_deltalake(
            f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}", 
            pagamento_final, 
            partition_by = ["year", "month", "day"],
            storage_options=storage_options,
            mode="overwrite",
            #overwrite_schema=True
        )
        logger.info("Salvamento concluído com sucesso.")

    except Exception as e:
        logger.error(f"Erro ao salvar as informações: {str(e)}")