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
                FROM minioraw.motor.laudos_alpe
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
                l.data_hora as data_execucao_laudo,
                l.chave_unica,
                l.nome_filtro,
                l.valor_aprovado as valor_aprovado_motor,
                l.score,
                l.restritivo_pj,
                l.restritivo_pf,
                l.total_restritivo,
                p.issue_key,
                l.politica,
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
                l.ramificacao as ramificacao_motor,
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
        valor_str = str(valor).strip().replace(',', '.')  # Normaliza string
        if valor_str == '':
           return None # Trata string vazia como None
        try:
            return Decimal(valor_str).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
        except Exception:
            # Caso algum valor ainda não seja convertido, retorna None ou erro
            return None
            
    if cruzamento_tabelas is not None and not cruzamento_tabelas.empty:
        # Aplicar a função nas colunas desejadas
        cruzamento_tabelas['valor_aprovado_motor'] = cruzamento_tabelas['valor_aprovado_motor'].apply(ajustar_decimal)
        cruzamento_tabelas['limite_pedido'] = cruzamento_tabelas['limite_pedido'].apply(ajustar_decimal)
        cruzamento_tabelas['limite_aprovado'] = cruzamento_tabelas['limite_aprovado'].apply(ajustar_decimal)
        cruzamento_tabelas['restritivo_pj'] = cruzamento_tabelas['restritivo_pj'].apply(ajustar_decimal)
        cruzamento_tabelas['restritivo_pf'] = cruzamento_tabelas['restritivo_pf'].apply(ajustar_decimal)
        cruzamento_tabelas['total_restritivo'] = cruzamento_tabelas['total_restritivo'].apply(ajustar_decimal)

        # Converter para float e arredondar para 2 casas decimais
        cruzamento_tabelas['valor_aprovado_motor'] = cruzamento_tabelas['valor_aprovado_motor'].astype(float).round(2)
        cruzamento_tabelas['limite_pedido'] = cruzamento_tabelas['limite_pedido'].astype(float).round(2)
        cruzamento_tabelas['limite_aprovado'] = cruzamento_tabelas['limite_aprovado'].astype(float).round(2)
        cruzamento_tabelas['restritivo_pj'] = cruzamento_tabelas['restritivo_pj'].astype(float).round(2)
        cruzamento_tabelas['restritivo_pf'] = cruzamento_tabelas['restritivo_pf'].astype(float).round(2)
        cruzamento_tabelas['total_restritivo'] = cruzamento_tabelas['total_restritivo'].astype(float).round(2)


        #Tratamento score para inteiro com suporte a nulos
        cruzamento_tabelas['score'] = pd.to_numeric(cruzamento_tabelas['score'], errors='coerce').astype('Int64')

        # Converte data_hora para date
        cruzamento_tabelas['data_execucao_laudo'] = pd.to_datetime(cruzamento_tabelas['data_execucao_laudo'], errors='coerce').dt.date

        # Timestamp e partições
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



