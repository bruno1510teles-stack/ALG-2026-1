# Carregando libs
import pandas as pd
import numpy as np
import re
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timezone, timedelta
from io import BytesIO
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from unidecode import unidecode
from airflow.models import Variable
from deltalake import write_deltalake


def segmentacao_carteira_refined (access_params=None,  **kwargs):

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
    

    #Query do trino
    query_segmentacao_carteira = f"""
        WITH segmento_atual AS 
        (
        SELECT
            SUBSTRING(REGEXP_REPLACE(cv.cnpj_sacado, '[^0-9]', ''),1,8) AS cnpj_raiz
            , CASE
            WHEN GREATEST(COALESCE(MAX(cv.carteira), 0), COALESCE(MAX(l.limite_atribuido), 0)) <= 100000 THEN '1 - VAREJO LIGHT'
            WHEN GREATEST(COALESCE(MAX(cv.carteira), 0), COALESCE(MAX(l.limite_atribuido), 0)) > 100000 AND GREATEST(COALESCE(MAX(cv.carteira), 0), COALESCE(MAX(l.limite_atribuido), 0)) <= 300000 THEN '2 - VAREJO'
            WHEN GREATEST(COALESCE(MAX(cv.carteira), 0), COALESCE(MAX(l.limite_atribuido), 0)) > 300000 AND GREATEST(COALESCE(MAX(cv.carteira), 0), COALESCE(MAX(l.limite_atribuido), 0)) <= 5000000 THEN '3 - MIDDLE'
            WHEN GREATEST(COALESCE(MAX(cv.carteira), 0), COALESCE(MAX(l.limite_atribuido), 0)) > 5000000 AND GREATEST(COALESCE(MAX(cv.carteira), 0), COALESCE(MAX(l.limite_atribuido), 0)) <= 10000000 THEN '4 - CORPORATE'
            WHEN GREATEST(COALESCE(MAX(cv.carteira), 0), COALESCE(MAX(l.limite_atribuido), 0)) > 10000000 THEN '5 - LARGE CORPORATE'
            ELSE 'SEM FAIXA' END AS segmentacao_atual
        FROM deltalakerefined.payments.carteira_vendermais cv
        LEFT JOIN deltalaketrusted.limites.limite l ON SUBSTRING(REGEXP_REPLACE(cv.cnpj_sacado, '[^0-9]', ''),1,8) = SUBSTRING(REGEXP_REPLACE(l.cnpj_sacado, '[^0-9]', ''),1,8)
        GROUP BY 1
        )
        SELECT
            cv.safra
            , SUBSTRING(REGEXP_REPLACE(cv.cnpj_sacado, '[^0-9]', ''),1,8) AS cnpj_raiz
            , cv.nome_sacado
            , cv.cnpj_cedente
            , cv.nome_cedente 
            , sa.segmentacao_atual
            , CASE 
                WHEN SUM(cv.carteira) <= 100000 THEN '1 - VAREJO LIGHT'
                WHEN SUM(cv.carteira) > 100000 AND SUM(cv.carteira) <= 300000 THEN '2 - VAREJO'
                WHEN SUM(cv.carteira) > 300000 AND SUM(cv.carteira) <= 5000000 THEN '3 - MIDDLE'
                WHEN SUM(cv.carteira) > 5000000 AND SUM(cv.carteira) <= 10000000 THEN '4 - CORPORATE'
                WHEN SUM(cv.carteira) > 10000000 THEN '5 - LARGE CORPORATE'
                ELSE 'SEM FAIXA' END AS segmentacao_safrada
            , SUM(cv.carteira) AS carteira
        FROM deltalakerefined.payments.carteira_vendermais cv
        LEFT JOIN segmento_atual sa ON sa.cnpj_raiz = SUBSTRING(REGEXP_REPLACE(cv.cnpj_sacado, '[^0-9]', ''),1,8)
        GROUP BY 1, 2, 3, 4, 5, 6
    """

    df_segmentacao_carteira = execute_query(conn, query_segmentacao_carteira)
    
    # Ajuste do decimal
    def ajustar_decimal(valor):
        if pd.isnull(valor):
            return None
        else:
            return Decimal(valor).quantize(Decimal('0.01'), rounding=ROUND_DOWN)

    # Aplicar a função nas colunas de valor
    df_segmentacao_carteira['carteira'] = df_segmentacao_carteira['carteira'].apply(ajustar_decimal).astype(float).round(2)


    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_segmentacao_carteira['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_segmentacao_carteira['year'], df_segmentacao_carteira['month'], df_segmentacao_carteira['day'] = now.year, now.month, now.day
    print("Tratamento dos dados concluído")

    #Removendo index
    df_segmentacao_carteira = df_segmentacao_carteira.reset_index(drop=True) 

    # Exportando dados para a camada refined
    ### Conectando na refined        
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_refined']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_REFINED = "carteira"
    FOLDER_DESTINATION_REFINED = "segmentacao_carteira"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}",
        df_segmentacao_carteira,
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )