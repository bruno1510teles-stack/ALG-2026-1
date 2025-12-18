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
    

    # Base Boletos com Enriquecimento

    query_boletos = f"""
            WITH boletos_base AS (
                SELECT
                    nfv.data AS data_ref,
                    CONCAT(
                        COALESCE(CAST(b.codigo_cedente AS VARCHAR), ''),
                        COALESCE(CAST(b.codigo_sacado AS VARCHAR), ''),
                        COALESCE(nfv.gerente_alpe, '')
                    ) AS chave_cliente_gerente,
            
                    b.codigo_cedente, 
                    b.nome_cedente, 
                    b.cnpj_cedente, 
                    b.codigo_sacado,
                    b.nome_sacado,
                    b.cnpj_sacado,
                    b.numero_nota_fiscal,
                    b.numero_nfe,
                    b.numero_titulo,
                    b.data_emissao,
                    b.data_efetivacao,
                    b.data_vencimento,
                    b.data_baixa,
                    b.status_titulo,
                    CAST(b.valor_face AS double) AS valor_face,
                    nfv.vendedor_fornecedor,
                    nfv.escritorio_venda,
                    nfv.gerente_alpe
            
                FROM deltalaketrusted.payments.boletos_internos b
                LEFT JOIN deltalaketrusted.payments.nota_fiscal_vendedor nfv
                    ON b.numero_nfe = nfv.numero_nfe
            ),
            boletos_enriquecidos AS (
                SELECT
                    data_ref,
                    codigo_cedente, 
                    nome_cedente, 
                    cnpj_cedente, 
                    codigo_sacado,
                    nome_sacado,
                    cnpj_sacado,
                    numero_nota_fiscal,
                    numero_nfe,
                    numero_titulo,
                    data_emissao,
                    data_efetivacao,
                    data_vencimento,
                    data_baixa,
                    status_titulo,
                    valor_face AS vop,
                    CASE WHEN status_titulo = 'A VENCER' THEN valor_face ELSE 0 END AS vop_a_vencer,
                    CASE WHEN status_titulo = 'VENCIDO' THEN valor_face ELSE 0 END AS vop_vencido,
                    valor_face - CASE WHEN status_titulo = 'A VENCER' THEN valor_face ELSE 0 END AS vop_performado,
                    CASE WHEN status_titulo = 'VENCIDO' THEN date_diff('day', data_vencimento, current_date) ELSE 0 END AS dias_em_atraso,
                    CASE WHEN status_titulo = 'VENCIDO' AND date_diff('day', data_vencimento, current_date) >= 30 THEN valor_face ELSE 0 END AS vop_over_30,
                    CASE WHEN status_titulo = 'VENCIDO' AND date_diff('day', data_vencimento, current_date) >= 60 THEN valor_face ELSE 0 END AS vop_over_60,
                    vendedor_fornecedor,
                    gerente_alpe,
                    escritorio_venda,
                    chave_cliente_gerente
                FROM boletos_base
            )
            
            SELECT *
            FROM boletos_enriquecidos
    """
    df_boletos_inad = execute_query(conn, query_boletos)
    print(f"Quantidade de linhas no DataFrame final: {df_boletos_inad.shape[0]}")

    # Timestamp e partições
    now = datetime.now(tz=timezone(timedelta(hours=-3)))

    df_boletos_inad['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_boletos_inad['year'], df_boletos_inad['month'], df_boletos_inad['day'] = now.year, now.month, now.day
    print("Tratamento dos dados concluído")

    # Exportando dados para a camada refined
    # Conectando na refined        
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_refined']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_REFINED = "planejamento-comercial"
    FOLDER_DESTINATION_REFINED = "bonus_inadimplencia"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}", 
        df_boletos_inad, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )