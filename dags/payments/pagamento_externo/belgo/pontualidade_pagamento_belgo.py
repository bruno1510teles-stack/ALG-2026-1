# Carregando libs
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from deltalake import write_deltalake
from datetime import datetime, timezone, timedelta
import numpy as np
import re
import logging
from airflow.utils.log.logging_mixin import LoggingMixin
from io import BytesIO
import re
from airflow.models import Variable


def pontualidade_pagamento_belgo (access_params=None,  **kwargs):

    # Carregando base de pagamento do Trino
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
    
    query_fat_belgo =  f"""
                        with base_tratada as (
                        
                            select
                                substring(cnpj, 1, 8) as cnpj_raiz,
                                valor as valor_fat,
                                case when (data_compensacao is null and dias_diff_pagamento > 0) then valor else 0 end as vlr_inad_corrente,
                                case when (data_compensacao is not null and dias_diff_pagamento > 0) then valor else 0 end as vlr_receb_atrasado,
                                
                                case when (data_compensacao is not null and dias_diff_pagamento >= 16 and dias_diff_pagamento <= 30) then valor else 0 end as vlr_receb_atrasado_16_a_30,
                                case when (data_compensacao is not null and dias_diff_pagamento >= 1 and dias_diff_pagamento <= 5) then valor else 0 end as vlr_receb_atrasado_1_a_5,
                                case when (data_compensacao is not null and dias_diff_pagamento >= 6 and dias_diff_pagamento <= 15) then valor else 0 end as vlr_receb_atrasado_6_a_15,
                                case when (data_compensacao is not null and dias_diff_pagamento > 30) then valor else 0 end as vlr_receb_atrasado_maior_30,
                                case when (data_compensacao is not null) then valor else 0 end as vlr_recebido,
                                
                                case when (data_compensacao is null) then valor else 0 end as vlr_total_titulo_aberto,
                                case when (data_compensacao is null and dias_diff_pagamento >= 16 and dias_diff_pagamento <= 30) then valor else 0 end as vlr_vencido_16_a_30,
                                case when (data_compensacao is null and dias_diff_pagamento >= 6 and dias_diff_pagamento <= 15) then valor else 0 end as vlr_vencido_6_a_15,
                                case when (data_compensacao is null and dias_diff_pagamento >= 1 and dias_diff_pagamento <= 5) then valor else 0 end as vlr_vencido_ate_5,
                                case when (data_compensacao is null and dias_diff_pagamento > 30) then valor else 0 end as vlr_vencido_maior_30,
                                
                                nome_cliente as razao_social
                                
                                
                            from (
                                    select
                                        *,
                                        date_diff('day', cb.vencimento_liquido, coalesce(cb.data_compensacao, DATE '2025-11-11')) AS dias_diff_pagamento
                                    from minioraw.planejamento_comercial.clientes_belgo as cb ) as sub
                        )
                        
                        select
                            cnpj_raiz as raiz_cnpj,
                            sum(valor_fat) as valor_fat,
                            sum(vlr_inad_corrente) as inad_corrente_total,
                            sum(vlr_receb_atrasado) as recebido_atrasado_total,
                            sum(vlr_recebido) as recebido_total,
                            sum(vlr_total_titulo_aberto) as em_aberto_total,
                            max(e.razao_social) as razao_social,
                            'BELGO' as fornecedor
                        from base_tratada as bt
                        left join (select  distinct
                                            cnpj_raiz as raiz_cnpj,
                                            razao_social
                                    from deltalaketrusted.receita_federal.empresas) as e
                        on bt.cnpj_raiz = e.raiz_cnpj 
                        group by 1
                        """

    df_belgo = execute_query(conn, query_fat_belgo)


    # Calcula Pontualidade
    df_belgo['pontualidade'] = (    
        df_belgo['recebido_total'] /
        (
            df_belgo['recebido_total'] + 
            df_belgo['inad_corrente_total'] + 
            df_belgo['recebido_atrasado_total']
        )
    ) * 100

    # Filtrando casos que tenha pelo menos algum recebido
    df_belgo = df_belgo[df_belgo['recebido_total'] > 0]


    # Filtrando colunas para base final
    colunas = ['raiz_cnpj', 'recebido_total', 'inad_corrente_total', 'recebido_atrasado_total', 'pontualidade']

    df_belgo_final = df_belgo[colunas].copy()


    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_belgo_final['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_belgo_final['year'], df_belgo_final['month'], df_belgo_final['day'] = now.year, now.month, now.day

    df_belgo_final = df_belgo_final.reset_index(drop=True)
    

    # Exportando dados para a camada Refined
    # # Conectando na Trusted
    logger = LoggingMixin().log 

    try:
        logger.info("Iniciando salvamento das informações")
        
        storage_options = {
            "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
            "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
            "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_refined']}",
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
        }

        # Definindo o caminho e salvando no MinIO
        BUCKET_SOURCE_REFINED = "motor"
        FOLDER_DESTINATION_REFINED = "pontualidade_belgo"

        write_deltalake(
            f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}", 
            df_belgo_final,
            partition_by = ["year", "month", "day"],
            storage_options=storage_options,
            mode="overwrite",
            #overwrite_schema=True
        )
        logger.info("Salvamento concluído com sucesso.")

    except Exception as e:
        logger.error(f"Erro ao salvar as informações: {str(e)}")