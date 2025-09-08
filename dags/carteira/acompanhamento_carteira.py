# Carregando libs
import pandas as pd
import numpy as np
import re
from datetime import datetime, timezone, timedelta
from io import BytesIO
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from unidecode import unidecode
from airflow.models import Variable
from deltalake import write_deltalake


def acompanhamento_carteira_refined (access_params=None,  **kwargs):
 
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
    query_carteira = f"""
    WITH carteira AS (
    	SELECT DISTINCT
    		cv.safra 
    		,substr(regexp_replace(cv.cnpj_sacado, '[^0-9]', ''), 1, 8) AS cnpj_raiz 
    		,sum (cv.carteira) AS carteira
    	FROM deltalakerefined.payments.carteira_vendermais cv
    	WHERE cv.safra >= date '2025-08-31'
    	GROUP BY 1, 2
    )
    SELECT DISTINCT 
    	c.safra
    	,substr(regexp_replace(bi.cnpj_sacado, '[^0-9]', ''), 1, 8) AS cnpj_raiz
    	,bi.nome_sacado
    	,l.limite_atribuido AS limite
    	,c.carteira
    	    ,CASE 
            WHEN l.limite_atribuido IS NULL OR l.limite_atribuido = 0 THEN NULL
            ELSE c.carteira / l.limite_atribuido
        END AS IU
    FROM deltalaketrusted.payments.boletos_internos bi
    LEFT JOIN deltalaketrusted.limites.limite l
    	ON substr(regexp_replace(bi.cnpj_sacado, '[^0-9]', ''), 1, 8) = substr(l.cnpj_sacado, 1, 8)
    LEFT JOIN carteira c
        ON substr(regexp_replace(bi.cnpj_sacado, '[^0-9]', ''), 1, 8) = c.cnpj_raiz
        """
       
    df_carteira = execute_query(conn, query_carteira)
    df_carteira = df_carteira.reset_index(drop=True)

    
    # Atribuindo data
    
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
 
    df_carteira['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_carteira['year'], df_carteira['month'], df_carteira['day'] = now.year, now.month, now.day
    print("Tratamento dos dados concluído")
 
 
    # Exportando dados para a camada refined
    # # Conectando na refined        
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_refined']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    
    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_REFINED = "carteira"
    FOLDER_DESTINATION_REFINED = "acompanhamento_carteira"
 
    write_deltalake(
        f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}",
        df_carteira,
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="append"
    )