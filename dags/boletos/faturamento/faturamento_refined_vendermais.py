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

def faturamento_to_refined(access_params=None,  **kwargs):

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
    with faturamento as (
    SELECT 
        cnpj_sacado, 
        nome_sacado, 
        cnpj_cedente, 
        nome_cedente, 
        numero_nfe, 
        data_fatura, 
        CONCAT(cnpj_sacado, cnpj_cedente, numero_nfe) as chave,
        SUM(valor_fatura) AS valor_fatura, 
        SUM(CASE WHEN status_fatura_sefaz <> 'CANCELED' OR status_fatura_sefaz IS NULL THEN valor_fatura ELSE 0 END) AS valor_fatura_pos_sefaz,
        SUM(CASE WHEN status_pago not in ('REJEITADO', 'EXCLUIDO', 'RECOMPRA ANTES DO PAGAMENTO') and (status_fatura_sefaz not in ('CANCELED') or status_fatura_sefaz is null) THEN valor_fatura ELSE 0 END) AS valor_fatura_oficial
    FROM 
        deltalaketrusted.payments.faturamento ft
    GROUP BY
        cnpj_sacado, nome_sacado, cnpj_cedente, nome_cedente, numero_nfe, data_fatura
    ),
    boletos as (
    SELECT
        regexp_replace(cnpj_sacado, '[./-]', '') AS cnpj_sacado, 
        nome_sacado, 
        regexp_replace(cnpj_cedente, '[./-]', '') AS cnpj_cedente,
        nome_cedente, 
        numero_nfe, 
        data_efetivacao, 
        CONCAT(regexp_replace(cnpj_sacado, '[./-]', ''), regexp_replace(cnpj_cedente, '[./-]', ''), numero_nfe) AS chave,
        SUM(valor_face) AS valor_face_qprof
    FROM 
        deltalaketrusted.payments.boletos_internos 
    GROUP BY
        regexp_replace(cnpj_sacado, '[./-]', ''), 
        nome_sacado, 
        regexp_replace(cnpj_cedente, '[./-]', ''), 
        nome_cedente, 
        numero_nfe,
        data_efetivacao
    )
    select 
        coalesce(ft.cnpj_sacado, bol.cnpj_sacado) as cnpj_sacado,  
        coalesce(ft.nome_sacado, bol.nome_sacado) as nome_sacado,
        coalesce(ft.cnpj_cedente, bol.cnpj_cedente) as cnpj_cedente,
        coalesce(ft.nome_cedente, bol.nome_cedente) as nome_cedente,
        coalesce(ft.numero_nfe, bol.numero_nfe) as numero_nfe,
        coalesce(bol.data_efetivacao, ft.data_fatura) as data,
        coalesce(ft.valor_fatura_total,0) as valor_fatura_total,
        coalesce(ft.valor_fatura_pos_sefaz, 0) as valor_fatura_pos_sefaz,
        coalesce(ft.valor_fatura_oficial, 0) as valor_fatura_oficial,
        coalesce(bol.valor_face_qprof, 0) as valor_face_qprof
    from 
        faturamento ft
        full join boletos bol on ft.chave = bol.chave
    """

    fatura = execute_query(conn, query_fatura)


    print(f"Quantidade de linhas no DataFrame 'fatura': {fatura.shape[0]}")


    # Função para ajustar os valores ao formato decimal(8, 2)
    def ajustar_decimal(valor):
        if pd.isnull(valor):
            return None  # Mantém valores nulos como estão
        else:
            # Limitar para no máximo 8 dígitos, com 2 casas decimais
            return Decimal(valor).quantize(Decimal('0.01'), rounding=ROUND_DOWN)

    # Aplicar a função na coluna 'valor_titulo'
    fatura['valor_fatura'] = fatura['valor_fatura'].apply(ajustar_decimal).astype(float).round(2)
    fatura['valor_fatura_pos_sefaz'] = fatura['valor_fatura_pos_sefaz'].apply(ajustar_decimal).astype(float).round(2)
    fatura['valor_fatura_oficial'] = fatura['valor_fatura_oficial'].apply(ajustar_decimal).astype(float).round(2)
    fatura['valor_face_qprof'] = fatura['valor_face_qprof'].apply(ajustar_decimal).astype(float).round(2)

    fatura['valor_fatura_total'] = fatura['valor_fatura_total'].astype(float).round(2)
    fatura['valor_fatura_oficial'] = fatura['valor_fatura_oficial'].astype(float).round(2)
    fatura['valor_fatura_pos_sefaz'] = fatura['valor_fatura_pos_sefaz'].astype(float).round(2)
    fatura['valor_face_qprof'] = fatura['valor_face_qprof'].astype(float).round(2)

    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))

    fatura['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    fatura['year'], fatura['month'], fatura['day'] = now.year, now.month, now.day
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
    BUCKET_SOURCE_REFINED = "payments"
    FOLDER_DESTINATION_REFINED = "faturamento"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}", 
        fatura, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )