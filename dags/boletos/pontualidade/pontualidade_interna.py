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

def pontualidade_to_refined (access_params=None,  **kwargs):
 
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

    
    # Query do trino
    query_pontualidade = f"""
    WITH pagamentos_com_status AS (
        SELECT 
            bi.cnpj_sacado
            , substring(regexp_replace(bi.cnpj_sacado, '[^0-9]', ''),1,8) AS cnpj_raiz
            , bi.nome_sacado
            , bi.status_titulo
            , bi.valor_face
            , bi.valor_baixado
            , bi.safra_vencimento 
            , bi.safra_baixa      
            , bi.data_vencimento 
        FROM deltalaketrusted.payments.boletos_internos bi
        WHERE bi.data_vencimento <= current_date
    ),
    agregacao_mensal AS (
        SELECT
            cnpj_raiz
            , nome_sacado
            , safra_vencimento AS safra_referencia
            , date_format(safra_vencimento, '%m/%Y') AS mes_ano_vencimento
            , SUM(p.valor_face) AS valor_face
            , SUM(CASE WHEN p.status_titulo = 'NO PRAZO' AND p.safra_baixa = p.safra_vencimento THEN p.valor_face ELSE 0 END) AS valor_pago_no_prazo
        FROM pagamentos_com_status p
        GROUP BY 1, 2, 3, 4
    ),
    pontualidade_acumulada AS (
        SELECT
            mes_ano_vencimento AS mes_ano
            , safra_referencia
            , cnpj_raiz
            , nome_sacado
            , SUM(valor_pago_no_prazo) OVER (PARTITION BY cnpj_raiz ORDER BY safra_referencia ROWS UNBOUNDED PRECEDING) AS valor_pago_no_prazo_acumulado
            , SUM(valor_face) OVER (PARTITION BY cnpj_raiz ORDER BY safra_referencia ROWS UNBOUNDED PRECEDING) AS valor_total_acumulado
        FROM agregacao_mensal
    )
    SELECT 
        mes_ano
        , safra_referencia
        , cnpj_raiz
        , nome_sacado
        , valor_pago_no_prazo_acumulado AS valor_pago_no_prazo
        , valor_total_acumulado AS vop_performado
        , ROUND(100.0 * valor_pago_no_prazo_acumulado / NULLIF(valor_total_acumulado, 0), 2) AS pontualidade
    FROM pontualidade_acumulada
    """
       
    df_pontualidade = execute_query(conn, query_pontualidade)

    # Ajuste do decimal
    def ajustar_decimal(valor):
        if pd.isnull(valor):
            return None
        else:
            return Decimal(valor).quantize(Decimal('0.01'), rounding=ROUND_DOWN)

    # Aplicar a função nas colunas de valor
    df_pontualidade['vop_performado'] = df_pontualidade['vop_performado'].apply(ajustar_decimal).astype(float).round(2)
    df_pontualidade['valor_pago_no_prazo'] = df_pontualidade['valor_pago_no_prazo'].apply(ajustar_decimal).astype(float).round(2)
    
    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_pontualidade['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_pontualidade['year'], df_pontualidade['month'], df_pontualidade['day'] = now.year, now.month, now.day
    print("Tratamento dos dados concluído")

    #Removendo index (obrigatorio)
    df_pontualidade = df_pontualidade.reset_index(drop=True) 
 
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
    BUCKET_SOURCE_REFINED = "payments"
    FOLDER_DESTINATION_REFINED = "pontualidade_interna"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}",
        df_pontualidade,
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )