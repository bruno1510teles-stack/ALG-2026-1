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

def boletos_trusted_to_refined(access_params=None,  **kwargs):

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
    query_boleto = f"""
        with boletos_yandeh as (
        select 
            cnpj, max(dif_dias) as dif_dias_yandeh
        from (
            select cnpj, 
                DATE_DIFF(
                    'day',
                    vencimento,
                    coalesce(DATE(data_baixa), DATE(NOW()))
                ) AS dif_dias
            from 
                s3alpeyandeh.yandeh.rm_boleto 
            where 
                vencimento < date(now())
        )
        group by 
            cnpj
        ),
        boletos_qprof_yandeh as(
        select
        cnpj, max(dif_dias) as dif_dias_qprof_yandeh
        from (
            select 
                cnpj_sacado as cnpj, 
                    DATE_DIFF(
                        'day',
                        data_vencimento,
                        coalesce(DATE(data_baixa), DATE(NOW()))
                    ) AS dif_dias	
            from
                deltalaketrusted.yandeh.boletos_internos
            where
                data_vencimento < date(now())
        )
        group by 
            cnpj
        )
        select
            coalesce(boly.cnpj, bolq.cnpj) as cnpj, coalesce(dif_dias_yandeh, 0) as dif_dias_yandeh, coalesce(dif_dias_qprof_yandeh, 0) as dif_dias_qprof_yandeh,
            case when 
                coalesce(dif_dias_yandeh, 0) > coalesce(dif_dias_qprof_yandeh, 0)
                THEN coalesce(dif_dias_yandeh, 0)
            ELSE coalesce(dif_dias_qprof_yandeh, 0)
            END AS max_entre_colunas
        from 
            boletos_yandeh boly
            full join boletos_qprof_yandeh bolq on boly.cnpj = bolq.cnpj
    """
    boleto = execute_query(conn, query_boleto)

    print(f"Quantidade de linhas no DataFrame 'boleto': {boleto.shape[0]}")

    df = boleto

    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df['year'], df['month'], df['day'] = now.year, now.month, now.day
    print("Tratamento dos dados concluído")

    print(f"Quantidade de linhas no DataFrame final: {df.shape[0]}")

    # Exportando dados para a camada Trusted
    # # Conectando na Trusted        
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_refined']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_REFINED = "payments"
    FOLDER_DESTINATION_REFINED = "yandeh/max_dias_vencidos"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}", 
        df, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )