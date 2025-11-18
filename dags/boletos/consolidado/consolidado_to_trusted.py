# Carregando libs
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from deltalake import write_deltalake
from datetime import datetime, timezone, timedelta
import os
from airflow.models import Variable
from decimal import Decimal, ROUND_DOWN


def consolidado_to_trusted(access_params=None,  **kwargs):

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
    

    # Query Trino
    query_boletos = f"""
        WITH limite AS 
            (
            SELECT
                l.cnpj_sacado 
                ,sum(l.limite_atribuido) AS limite_atribuido
                ,sum(l.limite_disponivel) AS limite_disponivel
            FROM deltalaketrusted.limites.limite l
            GROUP BY 1
            )
        SELECT DISTINCT 
            b.nome_cedente
            ,b.nome_sacado
            ,b.cnpj_sacado
            ,SUBSTR(REGEXP_REPLACE(b.cnpj_sacado, '[^0-9]', ''), 1, 8) AS raiz_cnpj
            ,b.cidade_sacado AS municipio
            ,b.uf_sacado
            ,CASE
                WHEN b.uf_sacado IN ('AC','AP','AM','PA','RO','RR','TO') THEN 'Norte'
                WHEN b.uf_sacado IN ('AL','BA','CE','MA','PB','PE','PI','RN','SE') THEN 'Nordeste'
                WHEN b.uf_sacado IN ('DF','GO','MT','MS') THEN 'Centro-Oeste'
                WHEN b.uf_sacado IN ('ES','MG','RJ','SP') THEN 'Sudeste'
                WHEN b.uf_sacado IN ('PR','RS','SC') THEN 'Sul'
                ELSE 'Desconhecida' END AS regiao
            ,nfv.vendedor_alpe
            ,nfv.vendedor_fornecedor
            ,nfv.escritorio_vendas AS filial
            ,cnae.segmento AS segmento_banco
            ,cnae.sub_segmento
            ,cnae.secao_final AS secao_final
            ,COUNT(DISTINCT CASE WHEN b.status_titulo = 'VENCIDO' THEN b.numero_titulo END) AS qtd_boletos_vencidos
            ,COUNT(DISTINCT CASE WHEN b.status_titulo = 'A VENCER' THEN b.numero_titulo END) AS qtd_boletos_a_vencer
            ,MAX(CASE WHEN b.status_titulo = 'VENCIDO' THEN DATE_DIFF('day', b.data_vencimento, CURRENT_DATE) ELSE NULL END) AS max_atraso
            ,MAX(CASE
                WHEN b.status_titulo = 'VENCIDO' AND DATE_DIFF('day', b.data_vencimento, CURRENT_DATE) <= 5 THEN '1 - 1 A 5 DIAS'
                WHEN b.status_titulo = 'VENCIDO' AND DATE_DIFF('day', b.data_vencimento, CURRENT_DATE) <= 15 THEN '2 - 6 A 15 DIAS'
                WHEN b.status_titulo = 'VENCIDO' AND DATE_DIFF('day', b.data_vencimento, CURRENT_DATE) <= 30 THEN '3 - 16 A 30 DIAS'
                WHEN b.status_titulo = 'VENCIDO' AND DATE_DIFF('day', b.data_vencimento, CURRENT_DATE) <= 60 THEN '4 - 31 A 60 DIAS'
                WHEN b.status_titulo = 'VENCIDO' AND DATE_DIFF('day', b.data_vencimento, CURRENT_DATE) <= 90 THEN '5 - 61 A 90 DIAS'
                WHEN b.status_titulo = 'VENCIDO' AND DATE_DIFF('day', b.data_vencimento, CURRENT_DATE) <= 120 THEN '6 - 91 A 120 DIAS'
                WHEN b.status_titulo = 'VENCIDO' AND DATE_DIFF('day', b.data_vencimento, CURRENT_DATE) <= 150 THEN '7 - 121 A 150 DIAS'
                WHEN b.status_titulo = 'VENCIDO' AND DATE_DIFF('day', b.data_vencimento, CURRENT_DATE) <= 180 THEN '8 - 151 A 180 DIAS'
                WHEN b.status_titulo = 'VENCIDO' AND DATE_DIFF('day', b.data_vencimento, CURRENT_DATE) > 180 THEN '9 - MAIOR QUE 180'
                ELSE NULL END) AS maior_faixa_atraso
        -- LIMITES
            ,l.limite_atribuido
            ,l.limite_disponivel
        -- CALCULOS BOLETOS
    --        ,SUM(CASE WHEN b.status_titulo = 'VENCIDO' THEN b.valor_face ELSE 0 END) AS vop_vencido
    --        ,SUM(CASE WHEN b.status_titulo = 'A VENCER' THEN b.valor_face ELSE 0 END) AS vop_a_vencer
            ,CAST(SUM(CASE WHEN b.status_titulo = 'VENCIDO' THEN b.valor_face ELSE 0 END) AS double) AS vop_vencido
            ,CAST(SUM(CASE WHEN b.status_titulo = 'A VENCER' THEN b.valor_face ELSE 0 END) AS double) AS vop_a_vencer
            ,SUM(CASE WHEN b.data_baixa IS NOT NULL THEN 1 ELSE 0 END) AS qtd_boletos_pagos
            ,ROUND(AVG(DATE_DIFF('day', b.data_vencimento, b.data_baixa)), 0) AS prazo_medio 
            ,CAST(ROUND(SUM(CASE WHEN b.data_baixa IS NULL THEN b.valor_face ELSE 0 END), 2) AS double) AS risco
            ,SUM(ROUND(
            CASE WHEN status_titulo = 'VENCIDO'
                THEN (CAST(0.04 AS double) / CAST(30 AS double))
                * CAST(GREATEST(date_diff('day', data_vencimento, CURRENT_DATE), 0) AS double)
                * CAST(valor_face AS double)
                ELSE NULL END, 2)) AS mora_atraso
            ,SUM(ROUND(
                CASE WHEN b.status_titulo = 'VENCIDO'
                THEN 0.02 * b.valor_face
                ELSE 0 END, 2)) AS multa_atraso
        -- VALOR ATUALIZADO
            ,(
                SUM (CASE WHEN b.status_titulo = 'VENCIDO' THEN b.valor_face ELSE 0 END)
                + SUM (ROUND (CASE WHEN b.status_titulo = 'VENCIDO' THEN (CAST (0.04 AS double) / CAST (30 AS double)) * CAST (GREATEST (date_diff ('day', b.data_vencimento, CURRENT_DATE), 0) AS double) * CAST (b.valor_face AS double) ELSE NULL END, 2))
                + SUM (ROUND (CASE WHEN b.status_titulo = 'VENCIDO' THEN CAST (0.02 AS double) * CAST (b.valor_face AS double) ELSE NULL END, 2))
            ) AS valor_atualizado
        FROM deltalaketrusted.payments.boletos_internos b
        LEFT JOIN deltalaketrusted.payments.nota_fiscal_vendedor nfv
            ON b.numero_nfe = nfv.numero_nfe
        LEFT JOIN deltalakerefined.cnae.depara_cnae cnae 
            ON cnae.cnpj_completo = LPAD(REGEXP_REPLACE(b.cnpj_sacado, '[./-]', ''), 14, '0')
        LEFT JOIN limite l ON REGEXP_REPLACE(b.cnpj_sacado, '[^0-9]', '') = l.cnpj_sacado
        GROUP BY
            b.nome_cedente
            ,b.nome_sacado
            ,b.cnpj_sacado
            ,SUBSTR(REGEXP_REPLACE(b.cnpj_sacado, '[^0-9]', ''), 1, 8)
            ,b.cidade_sacado
            ,b.uf_sacado
            ,nfv.vendedor_alpe
            ,nfv.vendedor_fornecedor
            ,nfv.escritorio_vendas
            ,cnae.segmento
            ,cnae.sub_segmento
            ,cnae.secao_final
            ,l.limite_atribuido
            ,l.limite_disponivel
            HAVING SUM(CASE WHEN b.status_titulo = 'VENCIDO' THEN b.valor_face ELSE 0 END) > 0
    """
    
    df_boletos = execute_query(conn, query_boletos)
    df_boletos = df_boletos.reset_index(drop=True)


    def ajustar_decimal(valor):
        if pd.isnull(valor):
            return None
        else:
            return Decimal(valor).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
    
    # Aplicar nas colunas
    df_boletos['vop_vencido'] = df_boletos['vop_vencido'].apply(ajustar_decimal)
    df_boletos['vop_a_vencer'] = df_boletos['vop_a_vencer'].apply(ajustar_decimal)
    df_boletos['risco'] = df_boletos['risco'].apply(ajustar_decimal)
    
    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_boletos['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_boletos['year'], df_boletos['month'], df_boletos['day'] = now.year, now.month, now.day
    print("Tratamento dos dados concluído")


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
    BUCKET_SOURCE_TRUSTED = "cobranca"
    FOLDER_DESTINATION_TRUSTED = "consolidado"
    
    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}", 
        df_boletos, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )