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

def get_laudos_com_propostas(access_params=None,  **kwargs):

    # Conectando ao Trino para Leitura
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
    
    # Definindo a consulta
    query_cruzamento_tabelas = """
            WITH laudos_rankeados AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY ticket_jira ORDER BY try_cast(data_hora AS timestamp) DESC) AS rn
                FROM minioraw.motor.laudos
                WHERE resolucao = 'MESA'
                AND try_cast(try_cast(data_hora AS timestamp) AS date) >= DATE '2025-04-14'
            ),
            laudos_filtrados AS (
                SELECT *
                FROM laudos_rankeados
                WHERE rn = 1
            ),
            propostas_filtradas AS (
                SELECT *
                FROM deltalaketrusted.jira.propostas
                WHERE categoria_decisor = 'MESA'
            )

            SELECT 
                l.*,
                p.issue_key,
                p.cnpj,
                p.raiz_cnpj,
                p.limite_pedido,
                p.limite_aprovado,
                p.gerente_tratado AS gerente_alpe,
                p.nome_vendedor_fn,
                p.filial_fn,
                p.analista_tratado AS analista_responsavel,
                p.cargo_analista,
                p.decisao,
                p.parecer,
                p.tipo_proposta
            FROM laudos_filtrados l
            INNER JOIN propostas_filtradas p
                ON l.ticket_jira = p.issue_key
    """
    cruzamento_tabelas = execute_query (conn, query_cruzamento_tabelas)

    # Função para ajustar os valores ao formato decimal(8, 2)
    def ajustar_decimal(valor):
        if pd.isnull(valor):
            return None  # Mantém valores nulos como estão
        else:
            # Limitar para no máximo 8 dígitos, com 2 casas decimais
            return Decimal(valor).quantize(Decimal('0.01'), rounding=ROUND_DOWN)

    if cruzamento_tabelas is not None and not cruzamento_tabelas.empty:
        # Aplica o ajuste decimal nas colunas
        cruzamento_tabelas['valor_aprovado'] = cruzamento_tabelas['valor_aprovado'].apply(ajustar_decimal).astype(float).round(2)
        cruzamento_tabelas['limite_pedido'] = cruzamento_tabelas['limite_pedido'].apply(ajustar_decimal).astype(float).round(2)
        cruzamento_tabelas['limite_aprovado'] = cruzamento_tabelas['limite_aprovado'].apply(ajustar_decimal).astype(float).round(2)

    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))

    cruzamento_tabelas['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    cruzamento_tabelas['year'], cruzamento_tabelas['month'], cruzamento_tabelas['day'] = now.year, now.month, now.day
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
    BUCKET_SOURCE_REFINED = "motor"
    FOLDER_DESTINATION_REFINED = "laudo"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}", 
        cruzamento_tabelas, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )



