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


def boletos_raw_to_refined_vop(access_params=None,  **kwargs):

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
    
    print('Importação da base...')

    query_boletos_trusted =  f"""
                            select "atualizado_em",
                                    "year",
                                    "month",
                                    "day",
                                    "data_emissao",
                                    "data_efetivacao",
                                    "data_vencimento",
                                    "data_baixa",
                                    "status_titulo",
                                    "status_liquidez",
                                    "safra_concessao",
                                    "safra_vencimento",
                                    "safra_baixa",
                                    "nome_cedente",
                                    "cnpj_cedente",
                                    "codigo_cedente",
                                    "cedente_id",
                                    "nome_sacado",
                                    "cnpj_sacado",
                                    "codigo_sacado",
                                    "sacado_id",
                                    "uf_sacado",
                                    "valor_face",
                                    "valor_titulo",
                                    "valor_baixado",
                                    "valor_desagio"
                            from deltalaketrusted.payments.boletos_internos
                            """

    df = execute_query(conn, query_boletos_trusted)

    print('Parte 1')

    ### Tratando colunas de data
    df['data_emissao']    = pd.to_datetime(df['data_emissao'], errors='coerce')
    df['data_efetivacao'] = pd.to_datetime(df['data_efetivacao'], errors='coerce')
    df['data_vencimento'] = pd.to_datetime(df['data_vencimento'], errors='coerce')
    df['data_baixa']      = pd.to_datetime(df['data_baixa'], errors='coerce')
    df['safra_concessao'] = pd.to_datetime(df['safra_concessao'], errors='coerce')
    df['safra_vencimento'] = pd.to_datetime(df['safra_vencimento'], errors='coerce')
    df['safra_baixa'] = pd.to_datetime(df['safra_baixa'], errors='coerce')

    df['data_baixa_aux'] = df['data_baixa'].fillna(pd.to_datetime('2262-04-11')) # Regra para tratar os casos que tem data_baixa NULL, pois usamos esse campo no calculo dos MOBs.

    # Calculando os valores
    df['vop'] = df['valor_face']
    df['vop_a_vencer'] = df.apply(lambda row: row['valor_face'] if row['status_titulo'] == 'A VENCER' else 0, axis=1)
    df['vop_performado'] = df['vop'] - df['vop_a_vencer']
    df['vencido'] = df.apply(lambda row: row['valor_face'] if row['status_titulo'] == 'VENCIDO' else 0, axis=1)

    print('Parte 2')

    # Calculando dias vencidos
    data_hoje = pd.Timestamp('today')

    df['dias_vencido'] = df.apply(lambda row: (data_hoje - row['data_vencimento']).days if row['status_titulo'] == 'VENCIDO' else 0, axis=1)


    # Calculando vop over30, over60 e over90
    df['vop_over_15'] = df.apply(
        lambda row: row['vencido'] if row['dias_vencido'] >= 15 else 0,
        axis=1
    )

    df['vop_over_30'] = df.apply(
        lambda row: row['vencido'] if row['dias_vencido'] >= 30 else 0,
        axis=1
    )

    df['vop_over_60'] = df.apply(
        lambda row: row['vencido'] if row['dias_vencido'] >= 60 else 0,
        axis=1
    )

    df['vop_over_90'] = df.apply(
        lambda row: row['vencido'] if row['dias_vencido'] >= 90 else 0,
        axis=1
    )

    print('Parte 3')

    # Calculando a diferença de dias entre a data de concessão e a data de referência
    df['dias_desde_concessao'] = (data_hoje - pd.to_datetime(df['data_efetivacao'])).dt.days


    # Criando datas dos MOBs para calculo dos indicadores
    df['prazo_medio'] = df['data_vencimento'] - df['data_emissao']

    df['M01'] = (df['safra_concessao'] + pd.DateOffset(months=2)) - pd.Timedelta(days=1)
    df['M02'] = (df['safra_concessao'] + pd.DateOffset(months=3)) - pd.Timedelta(days=1)
    df['M03'] = (df['safra_concessao'] + pd.DateOffset(months=4)) - pd.Timedelta(days=1)
    df['M04'] = (df['safra_concessao'] + pd.DateOffset(months=5)) - pd.Timedelta(days=1)
    df['M05'] = (df['safra_concessao'] + pd.DateOffset(months=6)) - pd.Timedelta(days=1)
    df['M06'] = (df['safra_concessao'] + pd.DateOffset(months=7)) - pd.Timedelta(days=1)

    df['vencimento_mais_15'] = df['data_vencimento'] + pd.Timedelta(days=15)
    df['vencimento_mais_30'] = df['data_vencimento'] + pd.Timedelta(days=30)
    df['vencimento_mais_60'] = df['data_vencimento'] + pd.Timedelta(days=60)
    df['vencimento_mais_90'] = df['data_vencimento'] + pd.Timedelta(days=90)


    # Calculando os MOBs 1, 2, 3, 4, 5 e 6 para cada Over
    def over15_mob(row, mob_num):
    
        if row[mob_num] > data_hoje or row['vencimento_mais_15'] > data_hoje:
            return 0

        elif (row[mob_num] > row['vencimento_mais_15']) and \
            (row['data_baixa_aux'] > row[mob_num]) and \
            (row['data_baixa_aux'] > row['vencimento_mais_15']):
            return row['valor_face']
        
        else:
            return 0


    def over30_mob(row, mob_num):
        
        if row[mob_num] > data_hoje or row['vencimento_mais_30'] > data_hoje:
            return 0

        elif (row[mob_num] > row['vencimento_mais_30']) and \
            (row['data_baixa_aux'] > row[mob_num]) and \
            (row['data_baixa_aux'] > row['vencimento_mais_30']):
            return row['valor_face']
        
        else:
            return 0


    def over60_mob(row, mob_num):
        
        if row[mob_num] > data_hoje or row['vencimento_mais_60'] > data_hoje:
            return 0

        elif (row[mob_num] > row['vencimento_mais_60']) and \
            (row['data_baixa_aux'] > row[mob_num]) and \
            (row['data_baixa_aux'] > row['vencimento_mais_60']):
            return row['valor_face']
        
        else:
            return 0


    def over90_mob(row, mob_num):
        
        if row[mob_num] > data_hoje or row['vencimento_mais_90'] > data_hoje:
            return 0

        elif (row[mob_num] > row['vencimento_mais_90']) and \
            (row['data_baixa_aux'] > row[mob_num]) and \
            (row['data_baixa_aux'] > row['vencimento_mais_90']):
            return row['valor_face']
        
        else:
            return 0


    print('Parte 4')


    # OVER 15 MOB...
    df['vop_over15_mob2'] = df.apply(lambda row: over15_mob(row, mob_num='M02'), axis=1)
    df['vop_over15_mob3'] = df.apply(lambda row: over15_mob(row, mob_num='M03'), axis=1)

    # OVER 30 MOB...
    df['vop_over30_mob1'] = df.apply(lambda row: over30_mob(row, mob_num='M01'), axis=1)
    df['vop_over30_mob2'] = df.apply(lambda row: over30_mob(row, mob_num='M02'), axis=1)
    df['vop_over30_mob3'] = df.apply(lambda row: over30_mob(row, mob_num='M03'), axis=1)
    df['vop_over30_mob4'] = df.apply(lambda row: over30_mob(row, mob_num='M04'), axis=1)
    df['vop_over30_mob5'] = df.apply(lambda row: over30_mob(row, mob_num='M05'), axis=1)
    df['vop_over30_mob6'] = df.apply(lambda row: over30_mob(row, mob_num='M06'), axis=1)

    # OVER 60 MOB...
    df['vop_over60_mob1'] = df.apply(lambda row: over60_mob(row, mob_num='M01'), axis=1)
    df['vop_over60_mob2'] = df.apply(lambda row: over60_mob(row, mob_num='M02'), axis=1)
    df['vop_over60_mob3'] = df.apply(lambda row: over60_mob(row, mob_num='M03'), axis=1)
    df['vop_over60_mob4'] = df.apply(lambda row: over60_mob(row, mob_num='M04'), axis=1)
    df['vop_over60_mob5'] = df.apply(lambda row: over60_mob(row, mob_num='M05'), axis=1)
    df['vop_over60_mob6'] = df.apply(lambda row: over60_mob(row, mob_num='M06'), axis=1)

    # OVER 90 MOB...
    df['vop_over90_mob1'] = df.apply(lambda row: over90_mob(row, mob_num='M01'), axis=1)
    df['vop_over90_mob2'] = df.apply(lambda row: over90_mob(row, mob_num='M02'), axis=1)
    df['vop_over90_mob3'] = df.apply(lambda row: over90_mob(row, mob_num='M03'), axis=1)
    df['vop_over90_mob4'] = df.apply(lambda row: over90_mob(row, mob_num='M04'), axis=1)
    df['vop_over90_mob5'] = df.apply(lambda row: over90_mob(row, mob_num='M05'), axis=1)
    df['vop_over90_mob6'] = df.apply(lambda row: over90_mob(row, mob_num='M06'), axis=1)

    print('Parte 5')

    # Filtrando apenas colunas para a Refined
    df_final = df[[ 'nome_cedente', 'cnpj_cedente', 'codigo_cedente', 'cedente_id', 'nome_sacado', 'cnpj_sacado', 'codigo_sacado', 
                'sacado_id', 'uf_sacado', 'data_emissao', 'data_efetivacao', 'data_vencimento', 'data_baixa', 'status_titulo', 
                'status_liquidez', 'safra_concessao', 'safra_vencimento', 'safra_baixa','valor_desagio', 'vop', 'vop_a_vencer', 
                'vop_performado', 'vencido', 'vop_over_15', 'vop_over_30', 'vop_over_60', 'vop_over_90', 'prazo_medio',
                'vop_over15_mob2', 'vop_over15_mob3', 'vop_over30_mob1', 'vop_over30_mob2', 'vop_over30_mob3', 'vop_over30_mob4',
                'vop_over30_mob5', 'vop_over30_mob6', 'vop_over60_mob1', 'vop_over60_mob2', 'vop_over60_mob3', 'vop_over60_mob4',
                'vop_over60_mob5', 'vop_over60_mob6', 'vop_over90_mob1', 'vop_over90_mob2', 'vop_over90_mob3', 'vop_over90_mob4',
                'vop_over90_mob5', 'vop_over90_mob6']].copy()
    

    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_final['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_final['year'], df_final['month'], df_final['day'] = now.year, now.month, now.day

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
        BUCKET_SOURCE_REFINED = "payments"
        FOLDER_DESTINATION_REFINED = "vop_vendermais"

        write_deltalake(
            f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}", 
            df_final, 
            partition_by=["year", "month", "day"],
            storage_options=storage_options,
            mode="overwrite"
        )
        logger.info("Salvamento concluído com sucesso.")

    except Exception as e:
        logger.error(f"Erro ao salvar as informações: {str(e)}")