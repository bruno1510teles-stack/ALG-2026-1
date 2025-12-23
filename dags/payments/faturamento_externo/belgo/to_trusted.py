# Importing Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import numpy as np
from airflow.models import Variable
import re
import logging
import base64, requests, json


def extracao_faturamento_externo_belgo(access_params=None, **kwargs):

    conn = connect(
        host='trino.alpe.com.br',
        port=443,
        user='vinicius_teixeira',
        auth=BasicAuthentication('vinicius_teixeira', 'TEjcv)-+b}o!QL5CM2:p'),
        http_scheme="https",)

    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        
        return pd.DataFrame(rows, columns=columns)
    

    query_belgo =       f"""
                    select * 
                    from minioraw.planejamento_comercial.clientes_belgo
                    """

    df_belgo = execute_query(conn, query_belgo)


    df_belgo["raiz_cnpj"] = df_belgo["cnpj"].str[:8]

    df_belgo["data_lancamento"] = pd.to_datetime(df_belgo["data_lancamento"])

    df_belgo["ano_mes"] = df_belgo["data_lancamento"].dt.strftime("%Y%m")


    # Pivotar Tabela

    df_pivot = (
        df_belgo
        .pivot_table(
            index="raiz_cnpj",
            columns="ano_mes",
            values="valor",
            aggfunc="sum"
        )
        .reset_index()
    )


    def tratar_colunas_202(df):
        # Filtrando colunas que começam com "202"
        colunas_202 = df.filter(like='202').columns

        # Para colunas numéricas: substituir valores negativos por 0
        for col in colunas_202:
            if df[col].dtype in ['int64', 'float64']:
                df[col] = df[col].clip(lower=0)  # Substitui valores negativos por 0
        
        # Para colunas de string: transformar todos os textos em maiúsculas
        for col in colunas_202:
            if df[col].dtype == 'object':  # Verifica se a coluna é de string (object)
                df[col] = pd.to_numeric(df[col].str.replace(' BRL', '').str.replace('.', '').str.replace(',', '.'), errors='coerce')
        
        return df

    tratar_colunas_202(df_pivot)

    # Treating NaN values
    def fillna_in_columns_starting_with(df, prefix, value):
        # Filtering columns
        filtered_columns = [col for col in df.columns if str(col).startswith(prefix)]
        
        df[filtered_columns] = df[filtered_columns].fillna(value)

    fillna_in_columns_starting_with(df_pivot, '20', 0)



    # Extraindo os CNPJs do DataFrame 'df_pivot' e convertendo-os para uma lista
    cnpjs = df_pivot['raiz_cnpj'].unique().tolist()

    tamanho = len(cnpjs) // 3

    cnpj_part_1 = cnpjs[:tamanho]
    cnpj_part_2 = cnpjs[tamanho:2*tamanho]
    cnpj_part_3 = cnpjs[2*tamanho:]

    # Convertendo a lista para uma string no formato adequado para o SQL
    cnpjs_str_1 = ', '.join([f"'{cnpj}'" for cnpj in cnpj_part_1])
    cnpjs_str_2 = ', '.join([f"'{cnpj}'" for cnpj in cnpj_part_2])
    cnpjs_str_3 = ', '.join([f"'{cnpj}'" for cnpj in cnpj_part_3])

    query_receita_1 = f"""
                    select
                        distinct
                        substr(identificador, 1, length(identificador) - 6) as raiz_cnpj,
                        municipio as cidade,
                        uf
                    from deltalaketrusted.pessoas_e_organizacoes.endereco
                    where year = 2024
                    and month = 6
                    and day = 5
                    and substr(identificador, 9, 4) = '0001'
                    and substr(identificador, 1, length(identificador) - 6) in ({cnpjs_str_1})
                    """

    query_receita_2 = f"""
                    select
                        distinct
                        substr(identificador, 1, length(identificador) - 6) as raiz_cnpj,
                        municipio as cidade,
                        uf
                    from deltalaketrusted.pessoas_e_organizacoes.endereco
                    where year = 2024
                    and month = 6
                    and day = 5
                    and substr(identificador, 9, 4) = '0001'
                    and substr(identificador, 1, length(identificador) - 6) in ({cnpjs_str_2})
                    """

    query_receita_3 = f"""
                        select
                            distinct
                            substr(identificador, 1, length(identificador) - 6) as raiz_cnpj,
                            municipio as cidade,
                            uf
                        from deltalaketrusted.pessoas_e_organizacoes.endereco
                        where year = 2024
                        and month = 6
                        and day = 5
                        and substr(identificador, 9, 4) = '0001'
                        and substr(identificador, 1, length(identificador) - 6) in ({cnpjs_str_3})
                        """

    receita_1 = execute_query(conn, query_receita_1)
    receita_2 = execute_query(conn, query_receita_2)
    receita_3 = execute_query(conn, query_receita_3)

    receita = pd.concat([receita_1, receita_2, receita_3], ignore_index=True)

    # Fazendo JOIN

    df_final = pd.merge(df_pivot, receita, on='raiz_cnpj', how='left')

    df_final = df_final.reset_index(drop=True)




    print('Pegando Razao Social do nosso banco de dados...')


    cnpjs = df_final['raiz_cnpj'].unique().tolist()

    tamanho = len(cnpjs) // 3

    cnpj_part_1 = cnpjs[:tamanho]
    cnpj_part_2 = cnpjs[tamanho:2*tamanho]
    cnpj_part_3 = cnpjs[2*tamanho:]

    # Convertendo a lista para uma string no formato adequado para o SQL
    cnpjs_str_1 = ', '.join([f"'{cnpj}'" for cnpj in cnpj_part_1])
    cnpjs_str_2 = ', '.join([f"'{cnpj}'" for cnpj in cnpj_part_2])
    cnpjs_str_3 = ', '.join([f"'{cnpj}'" for cnpj in cnpj_part_3])


    query_receita_1 =  f""" 
                        select  distinct
                                cnpj_raiz as raiz_cnpj,
                                razao_social
                        from deltalaketrusted.receita_federal.empresas
                        where cnpj_raiz in ({cnpjs_str_1})
                    """


    query_receita_2 =  f""" 
                        select  distinct
                                cnpj_raiz as raiz_cnpj,
                                razao_social
                        from deltalaketrusted.receita_federal.empresas
                        where cnpj_raiz in ({cnpjs_str_2})
                    """

    query_receita_3 =  f""" 
                        select  distinct
                                cnpj_raiz as raiz_cnpj,
                                razao_social
                        from deltalaketrusted.receita_federal.empresas
                        where cnpj_raiz in ({cnpjs_str_3})
                    """

    receita_1 = execute_query(conn, query_receita_1)
    receita_2 = execute_query(conn, query_receita_2)
    receita_3 = execute_query(conn, query_receita_3)

    receita = pd.concat([receita_1, receita_2, receita_3], ignore_index=True)

    df_final = pd.merge(
        df_final, 
        receita, 
        on=['raiz_cnpj'], 
        how='left'
    )

    df_final['razao_social'] = df_final['razao_social'].fillna('X')


    # Setando Unidade Consolidada
    df_final['unidade_consolidada'] = 'BELGO'


    # Reorganizando Colunas

    colunas_ordem = ['raiz_cnpj', 'razao_social', 'unidade_consolidada', 'cidade', 'uf'] + [col for col in df_final.columns if col not in ['raiz_cnpj', 'razao_social', 'unidade_consolidada', 'cidade', 'uf']]
    df_final = df_final[colunas_ordem]


    print('Exportando base...')

    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_final['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_final['year'], df_final['month'], df_final['day'] = now.year, now.month, now.day

    df_final = df_final.reset_index(drop=True)


    # Exportando dados para a camada Trusted
    # # Conectando na Trusted
    storage_options = {
        "AWS_ACCESS_KEY_ID": Variable.get("MINIO_TRUSTED_ACCESS_KEY"),
        "AWS_SECRET_ACCESS_KEY": Variable.get("MINIO_TRUSTED_SECRET_KEY"),
        "AWS_ENDPOINT_URL": f"https://{Variable.get('MINIO_TRUSTED_ENDPOINT')}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_TRUSTED = 'payments'
    FOLDER_DESTINATION_TRUSTED = 'faturamento_externo/belgo/base_atual'

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}", 
        df_final, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )
