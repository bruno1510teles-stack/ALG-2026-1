# Importing Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import numpy as np


def tratamento_pagamento_externo(access_params=None, **kwargs):

    # Conectando no Trino e validando

    # Coletando dados da camada Trusted
    # Conectando com o banco
    
    
    conn = connect(
        host=access_params['trino_endpoint'],
        port=access_params['trino_port'],
        user=access_params['trino_user'],
        auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
        http_scheme="https",
    )

    '''
    conn = connect(
        host='trino.alpe.com.br',
        port='443',
        user='trinodados',
        auth=BasicAuthentication('trinodados', 'hosgzPvuhyXkP<j}RyT+'),
        http_scheme="https",
    )
    '''

    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        
        return pd.DataFrame(rows, columns=columns)

    query_base_fat_externo_trusted = f"""
                                        select 
                                            *
                                        from deltalaketrusted.payments.pagamento_externo_arcelor
                                    """

    df = execute_query(conn, query_base_fat_externo_trusted)

    print(f"Quantidade de linhas no DataFrame 'dados': {df.shape[0]}")


    # Tratamento para Refined

    # Indicadores

    def somar_colunas_prefixo(df, prefixo, nome_coluna):
        # Filtrar as colunas que começam com 'vlr'
        colunas_filtradas = df.filter(like=prefixo).columns
        
        # Criar uma nova coluna somando apenas essas colunas
        df[nome_coluna] = df[colunas_filtradas].sum(axis=1)
        
        return df

    somar_colunas_prefixo(df, 'vlr_total_titulo_ab', 'valor_titulo_aberto_total')
    somar_colunas_prefixo(df, 'vlr_inad_corrente', 'valor_inad_corrente_total')
    somar_colunas_prefixo(df, 'vlr_receb_atrasado_202', 'valor_receb_atrasado_total')
    somar_colunas_prefixo(df, 'vlr_recebido_2', 'valor_recebido_total')

    df['valor_titulo_aberto_total'] = round(df['valor_titulo_aberto_total'], 2)
    df['valor_inad_corrente_total'] = round(df['valor_inad_corrente_total'], 2)
    df['valor_receb_atrasado_total'] = round(df['valor_receb_atrasado_total'], 2)
    df['valor_recebido_total'] = round(df['valor_recebido_total'], 2)

    print('Parte 1')

    # --> %Liquidez
    # --> %Pontualidade

    df["Liquidez%"] = round(((df['valor_recebido_total'] + df['valor_receb_atrasado_total']) / (df['valor_recebido_total'] + df['valor_receb_atrasado_total'] + df['valor_inad_corrente_total'])).astype(float),2)

    df['Pontualidade%'] = round(((df['valor_recebido_total']) / (df['valor_recebido_total'] + df['valor_receb_atrasado_total'] + df['valor_inad_corrente_total'])).astype(float),2)

    df['Inadimplente%'] = round((df['valor_inad_corrente_total'] / df['valor_titulo_aberto_total']).astype(float).fillna(0),2)

    print('Parte 2')

    df_final = df.filter(items=['raiz_cnpj', 'unidade_consolidada', 'razao_social', 'atualizado_em', 'year', 'month', 'day', 'valor_titulo_aberto_total',
                                'valor_inad_corrente_total', 'valor_receb_atrasado_total', 'valor_recebido_total', 'liquidez%', 'pontualidade%', 'inadimplente%'])

    # Exportando dados para a camada Refined
    # # Conectando na Refined
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_refined']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_REFINED = 'payments'
    FOLDER_DESTINATION_REFINED = 'pagamento_externo/arcelor'

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}", 
        df_final, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )