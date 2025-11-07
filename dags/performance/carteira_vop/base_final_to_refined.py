
# IMPORTANDO BIBLIOTECAS

from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timezone, timedelta
from trino.auth import BasicAuthentication
from deltalake import write_deltalake
from trino.dbapi import connect
from functools import reduce
from minio import Minio
from io import BytesIO
import pandas as pd
import numpy as np
import logging
import re


def performance_base_final (access_params=None,  **kwargs):


    # CONECTANDO COM O TRINO
    conn = connect(
        host=access_params['trino_endpoint'],
        port=access_params['trino_port'],
        user=access_params['trino_user'],
        auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
        http_scheme="https",
    )

    def execute_query(conn, query):
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()
        return pd.DataFrame(rows, columns=columns)
    

    # BASE VISÃO MOB

    query_base_mob =  f"""
                        select * 
                        from deltalakerefined.performance.visao_mob
                        """

    visao_mob = execute_query(conn, query_base_mob)
    print(f"Quantidade de linhas no DataFrame 'visao_mob': {visao_mob.shape[0]}")

    df_mob = visao_mob.copy()



    ## MOB1
    df_mob['over30mob1'] = np.where(
        df_mob['dias_em_atraso_mob1'] >= 30,
        df_mob['valor_vencido_mob1'],
        0
    )

    df_mob['over60mob1'] = np.where(
        df_mob['dias_em_atraso_mob1'] >= 60,
        df_mob['valor_vencido_mob1'],
        0
    )

    df_mob['over90mob1'] = np.where(
        df_mob['dias_em_atraso_mob1'] >= 90,
        df_mob['valor_vencido_mob1'],
        0
    )


    ## MOB2
    df_mob['over30mob2'] = np.where(
        df_mob['dias_em_atraso_mob2'] >= 30,
        df_mob['valor_vencido_mob2'],
        0
    )

    df_mob['over60mob2'] = np.where(
        df_mob['dias_em_atraso_mob2'] >= 60,
        df_mob['valor_vencido_mob2'],
        0
    )

    df_mob['over90mob2'] = np.where(
        df_mob['dias_em_atraso_mob2'] >= 90,
        df_mob['valor_vencido_mob2'],
        0
    )


    ## MOB3
    df_mob['over30mob3'] = np.where(
        df_mob['dias_em_atraso_mob3'] >= 30,
        df_mob['valor_vencido_mob3'],
        0
    )

    df_mob['over60mob3'] = np.where(
        df_mob['dias_em_atraso_mob3'] >= 60,
        df_mob['valor_vencido_mob3'],
        0
    )

    df_mob['over90mob3'] = np.where(
        df_mob['dias_em_atraso_mob3'] >= 90,
        df_mob['valor_vencido_mob3'],
        0
    )


    ## MOB4
    df_mob['over30mob4'] = np.where(
        df_mob['dias_em_atraso_mob4'] >= 30,
        df_mob['valor_vencido_mob4'],
        0
    )

    df_mob['over60mob4'] = np.where(
        df_mob['dias_em_atraso_mob4'] >= 60,
        df_mob['valor_vencido_mob4'],
        0
    )

    df_mob['over90mob4'] = np.where(
        df_mob['dias_em_atraso_mob4'] >= 90,
        df_mob['valor_vencido_mob4'],
        0
    )


    ## MOB5
    df_mob['over30mob5'] = np.where(
        df_mob['dias_em_atraso_mob5'] >= 30,
        df_mob['valor_vencido_mob5'],
        0
    )

    df_mob['over60mob5'] = np.where(
        df_mob['dias_em_atraso_mob5'] >= 60,
        df_mob['valor_vencido_mob5'],
        0
    )

    df_mob['over90mob5'] = np.where(
        df_mob['dias_em_atraso_mob5'] >= 90,
        df_mob['valor_vencido_mob5'],
        0
    )


    ## MOB6
    df_mob['over30mob6'] = np.where(
        df_mob['dias_em_atraso_mob6'] >= 30,
        df_mob['valor_vencido_mob6'],
        0
    )

    df_mob['over60mob6'] = np.where(
        df_mob['dias_em_atraso_mob6'] >= 60,
        df_mob['valor_vencido_mob6'],
        0
    )

    df_mob['over90mob6'] = np.where(
        df_mob['dias_em_atraso_mob6'] >= 90,
        df_mob['valor_vencido_mob6'],
        0
    )


    ## MOB7
    df_mob['over30mob7'] = np.where(
        df_mob['dias_em_atraso_mob7'] >= 30,
        df_mob['valor_vencido_mob7'],
        0
    )

    df_mob['over60mob7'] = np.where(
        df_mob['dias_em_atraso_mob7'] >= 60,
        df_mob['valor_vencido_mob7'],
        0
    )

    df_mob['over90mob7'] = np.where(
        df_mob['dias_em_atraso_mob7'] >= 90,
        df_mob['valor_vencido_mob7'],
        0
    )


    ## MOB8
    df_mob['over30mob8'] = np.where(
        df_mob['dias_em_atraso_mob8'] >= 30,
        df_mob['valor_vencido_mob8'],
        0
    )

    df_mob['over60mob8'] = np.where(
        df_mob['dias_em_atraso_mob8'] >= 60,
        df_mob['valor_vencido_mob8'],
        0
    )

    df_mob['over90mob8'] = np.where(
        df_mob['dias_em_atraso_mob8'] >= 90,
        df_mob['valor_vencido_mob8'],
        0
    )


    ## MOB9
    df_mob['over30mob9'] = np.where(
        df_mob['dias_em_atraso_mob9'] >= 30,
        df_mob['valor_vencido_mob9'],
        0
    )

    df_mob['over60mob9'] = np.where(
        df_mob['dias_em_atraso_mob9'] >= 60,
        df_mob['valor_vencido_mob9'],
        0
    )

    df_mob['over90mob9'] = np.where(
        df_mob['dias_em_atraso_mob9'] >= 90,
        df_mob['valor_vencido_mob9'],
        0
    )


    ## MOB10
    df_mob['over30mob10'] = np.where(
        df_mob['dias_em_atraso_mob10'] >= 30,
        df_mob['valor_vencido_mob10'],
        0
    )

    df_mob['over60mob10'] = np.where(
        df_mob['dias_em_atraso_mob10'] >= 60,
        df_mob['valor_vencido_mob10'],
        0
    )

    df_mob['over90mob10'] = np.where(
        df_mob['dias_em_atraso_mob10'] >= 90,
        df_mob['valor_vencido_mob10'],
        0
    )


    ## MOB11
    df_mob['over30mob11'] = np.where(
        df_mob['dias_em_atraso_mob11'] >= 30,
        df_mob['valor_vencido_mob11'],
        0
    )

    df_mob['over60mob11'] = np.where(
        df_mob['dias_em_atraso_mob11'] >= 60,
        df_mob['valor_vencido_mob11'],
        0
    )

    df_mob['over90mob11'] = np.where(
        df_mob['dias_em_atraso_mob11'] >= 90,
        df_mob['valor_vencido_mob11'],
        0
    )


    ## MOB12
    df_mob['over30mob12'] = np.where(
        df_mob['dias_em_atraso_mob12'] >= 30,
        df_mob['valor_vencido_mob12'],
        0
    )

    df_mob['over60mob12'] = np.where(
        df_mob['dias_em_atraso_mob12'] >= 60,
        df_mob['valor_vencido_mob12'],
        0
    )

    df_mob['over90mob12'] = np.where(
        df_mob['dias_em_atraso_mob12'] >= 90,
        df_mob['valor_vencido_mob12'],
        0
    )


    # BASE CLUSTER RECORRÊNCIA

    query_cluster_recorrencia =  f"""
                                select distinct data_ref, raiz_cnpj, cluster_nome 
                                from deltalakerefined.modelos.cluster_recorrencia
                                """

    df_cluster_recorrencia = execute_query(conn, query_cluster_recorrencia)
    print(f"Quantidade de linhas no DataFrame 'df_cluster_recorrencia': {df_cluster_recorrencia.shape[0]}")


    # TRANSFORMANDO E CRIANDO ANOMES_FECHAMENTO

    df_mob['fechamento'] = pd.to_datetime(df_mob['fechamento'], errors='coerce').dt.normalize()
    df_cluster_recorrencia['data_ref'] = pd.to_datetime(df_cluster_recorrencia['data_ref'], errors='coerce').dt.normalize()


    df_mob['anomes_fechamento'] = pd.to_datetime(df_mob['fechamento'], errors='coerce').dt.strftime('%Y-%m')
    df_cluster_recorrencia['anomes_fechamento'] = pd.to_datetime(df_cluster_recorrencia['data_ref'], errors='coerce').dt.strftime('%Y-%m')


    df_merged = df_mob.merge(
        df_cluster_recorrencia,
        left_on=['raiz_cnpj', 'anomes_fechamento'],
        right_on=['raiz_cnpj', 'anomes_fechamento'],
        how='left'
    )

    df_merged = df_merged.drop(columns=['data_ref'])



    # BASE FECHAMENTO HEMERA

    query_hemera =      f"""
                        select
                            substring(regexp_replace(cnpj_sacado, '[^0-9]', ''), 1, 8) as raiz_cnpj,
                            anomes_fechamento,
                            sum(pdd_inicial) as pdd_inicial,
                            sum(pdd_final) as pdd_final,
                            sum(delta_pdd) as delta_pdd,
                            sum(spread_liquido_fidc_valor) as spread_liquido_fidc,
                            sum(spread_alpe_inter) as spread_alpe_inter
                        from deltalakerefined.hemera_refined.fechamento f
                        group by 1, 2
                        """

    df_hemera = execute_query(conn, query_hemera)
    print(f"Quantidade de linhas no DataFrame 'df_hemera': {df_hemera.shape[0]}")


    df_final = df_merged.merge(
        df_hemera,
        left_on=['raiz_cnpj', 'anomes_fechamento'],
        right_on=['raiz_cnpj', 'anomes_fechamento'],
        how='left'
    )

    df_final = df_final.fillna(0)


    # DEFINE ORDEM DAS COLUNAS
    colunas_iniciais = [
        'fechamento', 'raiz_cnpj', 'cluster_nome',
        'pdd_inicial', 'pdd_final', 'delta_pdd',
        'spread_liquido_fidc', 'spread_alpe_inter',
        'vop_acumulado', 'vop_mes', 'vop_a_vencer_mes', 'carteira_mes'
    ]

    colunas_mob = []
    for i in range(1, 13):
        colunas_mob.extend([
            f'dias_em_atraso_mob{i}',
            f'valor_vencido_mob{i}',
            f'over30mob{i}',
            f'over60mob{i}',
            f'over90mob{i}'
        ])

    colunas_final = [c for c in colunas_iniciais + colunas_mob if c in df_final.columns]

    df_final = df_final[colunas_final]


    # TRANSFORMANDO FECHAMENTO EM DATE
    df_final['fechamento'] = pd.to_datetime(df_final['fechamento']).dt.date

    df_final['cluster_nome'] = df_final['cluster_nome'].astype(str)


    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_final['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_final['year'], df_final['month'], df_final['day'] = now.year, now.month, now.day

    df_final = df_final.reset_index(drop=True)

    print('Exportando base para Refined...')

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
        BUCKET_SOURCE_REFINED = "performance"
        FOLDER_DESTINATION_REFINED = "base_final"

        write_deltalake(
            f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}", 
            df_final, 
            partition_by=["year", "month", "day"],
            storage_options=storage_options,
            mode="overwrite",
            #overwrite_schema=True
        )
        logger.info("Salvamento concluído com sucesso.")

    except Exception as e:
        logger.error(f"Erro ao salvar as informações: {str(e)}")