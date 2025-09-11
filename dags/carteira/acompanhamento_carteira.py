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
    WITH limites AS (
        SELECT 
            SUBSTR(cnpj_sacado, 1, 8) AS raiz_cnpj
            ,SUM(limite_atribuido) AS limite_atribuido
            ,SUM(limite_utilizado) AS limite_utilizado
            ,SUM(limite_disponivel) AS limite_disponivel
        FROM deltalaketrusted.limites.limite
        GROUP BY substr(cnpj_sacado, 1, 8)
    )
    SELECT 
        SUBSTR(REGEXP_REPLACE(bi.cnpj_sacado, '[^0-9]', ''), 1, 8) AS raiz_cnpj
        ,bi.nome_sacado 
        ,SUM(bi.valor_titulo) AS carteira
        ,l.limite_atribuido
        ,l.limite_utilizado
        ,l.limite_disponivel
        ,CASE 
            WHEN l.limite_atribuido IS NULL OR l.limite_atribuido = 0 THEN NULL
            ELSE ROUND(SUM(bi.valor_titulo) / l.limite_atribuido, 5)
        END AS IU
    FROM deltalaketrusted.payments.boletos_internos bi
    LEFT JOIN limites l ON SUBSTR(REGEXP_REPLACE(bi.cnpj_sacado, '[^0-9]', ''), 1, 8) = l.raiz_cnpj
    WHERE bi.status_titulo <> 'NO PRAZO'
    AND bi.valor_titulo > 0
    GROUP BY 
        SUBSTR(REGEXP_REPLACE(bi.cnpj_sacado, '[^0-9]', ''), 1, 8)
        ,bi.nome_sacado 
        ,l.limite_atribuido
        ,l.limite_utilizado
        ,l.limite_disponivel
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