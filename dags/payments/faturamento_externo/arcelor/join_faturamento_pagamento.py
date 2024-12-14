# Importanto Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import numpy as np
import unicodedata


def join_faturamento_pagamento(access_params=None, **kwargs):

    # Conectando com o banco de dados Trino
    conn = connect(
        host='trino.alpe.com.br',
        port='443',
        user='trinodados',
        auth=BasicAuthentication('trinodados', 'hosgzPvuhyXkP<j}RyT+'),
        http_scheme="https",
    )


    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        
        return pd.DataFrame(rows, columns=columns)

    # Query Faturamento Trusted
    query_fat_trusted = f"""
                        select 
                            * 
                        from deltalaketrusted.payments.faturamento_externo_arcelor
                        """

    fat_trusted = execute_query(conn, query_fat_trusted)

    print(f"Quantidade de linhas no DataFrame 'boleto': {fat_trusted.shape[0]}")

    # Query Pagamento Trusted
    query_pag_trusted = f"""
                        select 
                            * 
                        from deltalaketrusted.payments.pagamento_externo_arcelor
                        """

    pag_trusted = execute_query(conn, query_pag_trusted)

    print(f"Quantidade de linhas no DataFrame 'boleto': {pag_trusted.shape[0]}")

    # Tratando Faturamento Trusted

    fat_trusted = fat_trusted.drop(columns=['razao_social', 'atualizado_em', 'year', 'month', 'day'])

    def remover_acentos(texto):
        if isinstance(texto, str):
            return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
        return texto

    fat_trusted['unidade'] = fat_trusted['unidade'].apply(remover_acentos)

    # Agrupando Faturamento Trusted
    fat_trusted = fat_trusted.groupby(['raiz_cnpj', 'unidade'], as_index=False).sum()


    # Pivot Faturamento Trusted
    fat_trusted = fat_trusted.melt(id_vars=['raiz_cnpj', 'unidade'], 
                                    var_name='anomes', 
                                    value_name='valor_fat')

    # Tratando Pagamento Trusted

    pag_trusted = pag_trusted.drop(columns=['atualizado_em', 'year', 'month', 'day'])

    def remover_acentos(texto):
        if isinstance(texto, str):
            return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
        return texto

    pag_trusted['unidade'] = pag_trusted['unidade'].apply(remover_acentos)

    # Agrupando Pagamento Trusted
    pag_trusted = pag_trusted.groupby(['raiz_cnpj', 'unidade'], as_index=False).sum()


    # Pivot Pagamento Trusted
    pag_trusted = pag_trusted.melt(id_vars=['raiz_cnpj', 'unidade'], 
                                    value_vars=[col for col in pag_trusted.columns if col not in ['raiz_cnpj', 'unidade']], 
                                    var_name='auxiliar', 
                                    value_name='valor_pag')


    pag_trusted['anomes'] = pag_trusted['auxiliar'].str[-6:]  # Ano no final

    pag_trusted['tipo_valor'] = pag_trusted['auxiliar'].str[:-7]   # Mês no final

    pag_trusted = pag_trusted.drop(columns=['auxiliar'])

    pag_trusted = pag_trusted.pivot_table(index=['raiz_cnpj', 'unidade', 'anomes'], 
                                columns='tipo_valor', 
                                values='valor_pag', 
                                aggfunc='sum').reset_index()

    pag_trusted = pag_trusted.drop(columns=['prazo_medio_atrasado'])

    # Merge Faturamento e Pagamento

    fat_pag = pd.merge(
        fat_trusted, 
        pag_trusted, 
        on=['raiz_cnpj', 'unidade', 'anomes'], 
        how='outer'
    )

    fat_pag = fat_pag.fillna(0)

    # Incluindo Razao Social Sacado

    # Extraindo os CNPJs do DataFrame 'fat_pag' e convertendo-os para uma lista
    cnpjs = fat_pag['raiz_cnpj'].unique().tolist()

    tamanho = len(cnpjs) // 2

    cnpj_part_1 = cnpjs[:tamanho]
    cnpj_part_2 = cnpjs[tamanho:]

    # Convertendo a lista para uma string no formato adequado para o SQL
    cnpjs_str_1 = ', '.join([f"'{cnpj}'" for cnpj in cnpj_part_1])
    cnpjs_str_2 = ', '.join([f"'{cnpj}'" for cnpj in cnpj_part_2])


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

    receita_1 = execute_query(conn, query_receita_1)
    receita_2 = execute_query(conn, query_receita_2)

    receita = pd.concat([receita_1, receita_2], ignore_index=True)

    fat_pag = pd.merge(
        fat_pag, 
        receita, 
        on=['raiz_cnpj'], 
        how='left'
    )

    fat_pag['razao_social'] = fat_pag['razao_social'].fillna('')


    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    fat_pag['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    fat_pag['year'], fat_pag['month'], fat_pag['day'] = now.year, now.month, now.day

    print('Exportando...')

    fat_pag = fat_pag.reset_index(drop=True)

    # Exportando dados para a camada Trusted
    # # Conectando na Trusted
    storage_options = {
        "AWS_ACCESS_KEY_ID": 'nr0qPLaAcdCtt7lAV4oa',
        "AWS_SECRET_ACCESS_KEY": 'GRA8FxnVMy7pGDvKP1wZK2nPOC3vP7F1AvH2u3Ch',
        "AWS_ENDPOINT_URL":"https://api-trusted.alpe.com.br",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_TRUSTED = 'payments'
    FOLDER_DESTINATION_TRUSTED = 'faturamento_externo/fat_pag'

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}", 
        fat_pag, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )