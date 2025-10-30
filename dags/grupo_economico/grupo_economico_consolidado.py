# Carregando libs
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from deltalake import write_deltalake
from datetime import datetime, timezone, timedelta
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from decimal import Decimal, ROUND_DOWN
import logging
import os
import pandas as pd

def ge_consolidado_to_refined (access_params=None,  **kwargs):

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
        cur.close()
        return pd.DataFrame(rows, columns=columns)

    
    # Query do trino
    query_ge_consolidado = f"""
        WITH limites AS (
            SELECT
            SUBSTRING(l.cnpj_sacado, 1, 8) AS raiz_cnpj
            , SUM (l.limite_atribuido) AS limite_atribuido
            , SUM (l.limite_utilizado) AS limite_utilizado
            , SUM (l.limite_disponivel) AS limite_disponivel
            FROM deltalaketrusted.limites.limite l
            GROUP BY 1
        ),
        carteira AS (
            SELECT
                SUBSTRING(REGEXP_REPLACE(cv.cnpj_sacado, '[^0-9]', ''), 1, 8) AS raiz_cnpj,
                SUM(cv.carteira) AS carteira_total,
                SUM(cv.carteira_vencida) AS carteira_vencida
            FROM deltalakerefined.payments.carteira_vendermais cv
            WHERE cv.safra = CURRENT_DATE
            GROUP BY 1
        ),
        boletos_internos AS (
            SELECT
                SUBSTRING(REGEXP_REPLACE(bi.cnpj_sacado , '[^0-9]', ''), 1, 8) AS raiz_cnpj,
                SUM(CASE WHEN bi.status_titulo NOT IN ('A VENCER') AND BI.data_vencimento <= CURRENT_DATE THEN bi.valor_face ELSE 0 END) AS vop_performado,
                MAX(CASE WHEN bi.status_titulo = 'VENCIDO' THEN DATE_DIFF('day', bi.data_vencimento, CURRENT_DATE) ELSE 0 END) AS dias_atraso,
                SUM(CASE WHEN bi.status_titulo = 'VENCIDO' AND DATE_DIFF('day', bi.data_vencimento, CURRENT_DATE) >= 15 THEN CAST(REPLACE(CAST(bi.valor_face AS VARCHAR), ',', '.') AS DOUBLE) ELSE 0 END) AS over_15
            FROM deltalaketrusted.payments.boletos_internos bi
            GROUP BY 1
        ),
        receita_federal AS
        (
            SELECT 
                rf.cnpj_raiz
                , SUM (rf.capital_social_empresa) AS capital_social
            FROM deltalakerefined.receita_federal.dados_cadastrais rf
            WHERE rf.flag_matriz = 'Sim'
            AND rf.situacao_cadastral = 'ATIVA'
            GROUP BY 1
        )
        SELECT
        -- grupo economico
            ge.raiz_cnpj
            , ge.razao_social
            , ge.nome_grupo
        -- receita_federal
            , capital_social
        -- limites
            , l.limite_atribuido
            , l.limite_utilizado
            , l.limite_disponivel
        -- carteira
            , carteira_total
            , carteira_vencida
        -- boletos internos
            , vop_performado
            , over_15
            , dias_atraso
        FROM deltalaketrusted.grupo_economico.grupo_economico ge
        LEFT JOIN limites l ON l.raiz_cnpj = ge.raiz_cnpj
        LEFT JOIN carteira c ON c.raiz_cnpj = ge.raiz_cnpj
        LEFT JOIN boletos_internos bi ON bi.raiz_cnpj = ge.raiz_cnpj
        LEFT JOIN receita_federal rf ON rf.cnpj_raiz = ge.raiz_cnpj 
    """

    df_ge_consolidado = execute_query(conn, query_ge_consolidado)

    # Ajustando decimais
    colunas_valor = [
        'capital_social', 'limite_atribuido', 'limite_utilizado', 'limite_disponivel', 
        'carteira_total', 'carteira_vencida', 'vop_performado', 'over_15'
    ]

    for col in colunas_valor:
        df_ge_consolidado[col] = (
            df_ge_consolidado[col]
            .apply(lambda x: None if pd.isnull(x) else Decimal(x).quantize(Decimal('0.01'), rounding=ROUND_DOWN))
            .astype(float)
            .round(2)
        )
    
    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_ge_consolidado['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_ge_consolidado['year'], df_ge_consolidado['month'], df_ge_consolidado['day'] = now.year, now.month, now.day
    print("Tratamento dos dados concluído")

    #Removendo index
    df_ge_consolidado = df_ge_consolidado.reset_index(drop=True) 

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
    BUCKET_SOURCE_REFINED = "grupo-economico"
    FOLDER_DESTINATION_REFINED = "grupo_economico_consolidado"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}",
        df_ge_consolidado,
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )