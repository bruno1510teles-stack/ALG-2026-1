# Carregando libs
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from deltalake import write_deltalake
from datetime import datetime, timezone, timedelta
import os
from airflow.models import Variable
import logging
from airflow.utils.log.logging_mixin import LoggingMixin
from decimal import Decimal, ROUND_DOWN
import numpy as np

def faturamento_to_trusted(access_params=None,  **kwargs):

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
    

    # Base Boletos CCRED
    query_fatura = f"""
                    select
                        faturamento_id AS id,
                        chave_nfe AS numero_nfe,
                        pedido_id AS numero_pedido,
                        cnpj_sacado AS cnpj_sacado,
                        razao_social_sacado AS nome_sacado,
                        cnpj_cedente AS cnpj_cedente,
                        razao_social_cedente AS nome_cedente,
                        date(data_faturamento) AS data_fatura,
                        valor_face AS valor_fatura,
                        upper(status_pedido) AS status_fatura,
                        upper(status_nfe) AS status_fatura_sefaz,
                        upper(pago) as status_pago
                    from postgres.ccred_schema_{Variable.get('STAGE')}_default.vw_pedido_faturamento
                    where (
                            pgid not in ('bariloche', 'ltcarol', 'hortmix', 'blow', 'ocean', 'philipmorris', 'caboclo',
                                        'benassi', 'seugil', 'roge', 'ltxando', 'comprefacil', 'adoro', 'girotrade', 'embala', 'ultracheese')
                            or pgid is null
                    )
                    """
    
    fatura = execute_query(conn, query_fatura)

    print(f"Quantidade de linhas no DataFrame 'fatura': {fatura.shape[0]}")

    # Ajuste do decimal
    def ajustar_decimal(valor):
        if pd.isnull(valor):
            return None
        else:
            return Decimal(valor).quantize(Decimal('0.01'), rounding=ROUND_DOWN)

    # Aplicar a função na coluna 'valor_titulo'
    fatura['valor_fatura'] = fatura['valor_fatura'].apply(ajustar_decimal).astype(float).round(2)

    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))

    fatura['valor_fatura'] = fatura['valor_fatura'].astype(float).round(2)

    # Timestamp e partições
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    fatura['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    fatura['year'], fatura['month'], fatura['day'] = now.year, now.month, now.day

    print("Tratamento dos dados concluído")

    # Configuração do Delta Lake
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    BUCKET_SOURCE_TRUSTED = "payments"
    FOLDER_DESTINATION_TRUSTED = "faturamento"

    # Escrevendo no Delta Lake com schema fixado
    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}",
        fatura,
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )