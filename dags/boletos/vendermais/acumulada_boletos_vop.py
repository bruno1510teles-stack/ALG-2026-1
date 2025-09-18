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


def boletos_raw_to_trusted_acumulada (access_params=None,  **kwargs):

    ### CONECTANDO COM O TRINO

    conn = connect(
        host=access_params['trino_endpoint'],
        port=access_params['trino_port'],
        user=access_params['trino_user'],
        auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
        http_scheme="https",)
    

    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        return pd.DataFrame(rows, columns=columns)
    


    ### QUERY BOLETOS

    query_boletos = f"""
                    select distinct 
                        substring(replace(replace(replace(cnpj_sacado, '.', ''), '/', ''), '-', ''), 1, 8) as raiz_cnpj,
                        *
                    from deltalaketrusted.payments.boletos_internos
                    """

    df_boletos = execute_query (conn, query_boletos)


    ### CRIANDO COLUNA ANO MÊS PARA FAZER O LOOP E ACUMULAR

    df_boletos["safra_concessao"] = pd.to_datetime(df_boletos["safra_concessao"], errors="coerce")

    df_boletos["ano_mes"] = df_boletos["safra_concessao"].dt.to_period("M")

    df_boletos = df_boletos.sort_values("ano_mes")


    ### MESES ÚNICOS PARA RODAR LÓGICA

    meses = df_boletos["ano_mes"].unique()


    ### LOOP PARA ACUMULAR

    bases_acumuladas = []

    for i, mes in enumerate(meses, start = 1):
        base = df_boletos[df_boletos["ano_mes"] <= mes].copy()
        base["data_ref"] = mes
        bases_acumuladas.append(base)

    ### CONCATENAR TODOS OS DADOS

    df_acumulado_boletos = pd.concat(bases_acumuladas, ignore_index=True)

    ### DELETANDO COLUNA ANO_MES

    df_acumulado_boletos = df_acumulado_boletos.drop(columns=["ano_mes"])

    ### PRIORIZANDO COLUNA DE DATA-REF

    colunas = ["data_ref"] + [c for c in df_acumulado_boletos.columns if c != "data_ref"]
    df_acumulado_boletos = df_acumulado_boletos[colunas]

    df_acumulado_boletos['data_ref'] = pd.to_datetime(df_acumulado_boletos['data_ref']).dt.to_period('M').dt.to_timestamp()


    ### RESET INDEX
    df_acumulado_boletos = df_acumulado_boletos.reset_index(drop=True)
    

    print(f"Quantidade de linhas no DataFrame final: {df_acumulado_boletos.shape[0]}")

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
    BUCKET_SOURCE_TRUSTED = "payments"
    FOLDER_DESTINATION_TRUSTED = "boletos_internos_acumulada"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}", 
        df_acumulado_boletos, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )