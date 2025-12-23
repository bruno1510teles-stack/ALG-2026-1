# Importing Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import numpy as np
from airflow.models import Variable
import re
import logging


def consolida_faturamento_externo (access_params=None, **kwargs):

    conn = connect(
        host=Variable.get("TRINO_ENDPOINT"),
        port=Variable.get("TRINO_PORT"),
        user=Variable.get("TRINO_USER"),
        auth=BasicAuthentication(Variable.get("TRINO_USER"), Variable.get("TRINO_PASSWORD")),
        http_scheme="https",
    )

    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        
        return pd.DataFrame(rows, columns=columns)
    

    # FATURAMENTO EXTERNO ARCELOR
    query_arcelor =       f"""
                        select * 
                        from deltalakerefined.payments.faturamento_externo_arcelor
                        """

    df_arcelor = execute_query(conn, query_arcelor)

    df_arcelor['unidade_consolidada'] = 'ARCELOR'

    # FATURAMENTO EXTERNO BELGO
    query_belgo =       f"""
                        select * 
                        from deltalakerefined.payments.faturamento_externo_belgo
                        """

    df_belgo = execute_query(conn, query_belgo)


    # CONCATENAR OS DOIS DFs

    df_all = pd.concat(
        [df_arcelor, df_belgo],
        ignore_index=True
    )

    df_all["unidade_consolidada"] = df_all["unidade_consolidada"].astype(str)


    df_final = (
        df_all
        .groupby("raiz_cnpj", as_index=False)
        .agg(
            vop_2023=("vop_2023", "sum"),
            vop_2024=("vop_2024", "sum"),
            vop_2025=("vop_2025", "sum"),
            vop_2026=("vop_2026", "sum"),
            vop_total=("vop_total", "sum"),
            max_vop_total=("max_vop_total", "max"),
            fornecedores=(
                "unidade_consolidada",
                lambda x: ", ".join(sorted(set(x)))
            )
        )
    )


    print('Exportando base...')

    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_final['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_final['year'], df_final['month'], df_final['day'] = now.year, now.month, now.day

    df_final = df_final.reset_index(drop=True)



    # Exportando dados para a camada Refined
    # # Conectando na Refined
    storage_options = {
        "AWS_ACCESS_KEY_ID": Variable.get("MINIO_REFINED_ACCESS_KEY"),
        "AWS_SECRET_ACCESS_KEY": Variable.get("MINIO_REFINED_SECRET_KEY"),
        "AWS_ENDPOINT_URL": f"https://{Variable.get('MINIO_REFINED_ENDPOINT')}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }


    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_REFINED = 'payments'
    FOLDER_DESTINATION_REFINED = 'faturamento_externo/consolidado'

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}", 
        df_final, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )