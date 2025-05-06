# Carregando libs
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import os, pytz
from datetime import datetime
import time
import base64
import requests
from airflow.models import Variable

def anti_fraude(access_params=None,  **kwargs):

    ### Configurando configurações necessárias
    # Trino
    conn = connect(
        host=access_params['trino_endpoint'],
        port=access_params['trino_port'],
        user=access_params['trino_user'],
        auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
        http_scheme="https",
    )
    # Pegando DF tarefa anterior
    # Recupera o objeto ti (task instance) via kwargs
    ti = kwargs['ti']
    base_analisar_dict  = ti.xcom_pull(task_ids='captura_proposta')
    base_analisar = pd.DataFrame(base_analisar_dict)

    ### Tratando base
    base_analisar['cnpj_sem_formatacao'] = base_analisar['CNPJ'].str.zfill(14)
    cnpj_analisar = base_analisar['cnpj_sem_formatacao'].unique()

    cnpj_analisar_str = ', '.join([f"'{cnpj}'" for cnpj in cnpj_analisar])

    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        return pd.DataFrame(rows, columns=columns)

    # Base auxiliar CNAE
    query_antifraude = f"""
    select 
        cnpj_sem_formatacao, flag_mudanca_endereco, flag_mudanca_cidade, flag_mudanca_estado, flag_endereco_igual 
    from 
        deltalakerefined.antifraude.alteracao_endereco 
    where 
        cnpj_sem_formatacao IN ({cnpj_analisar_str})
    """
    df_antifraude = execute_query(conn, query_antifraude)


    # Iniciando Tratamento

    df_antifraude['ramificacao_antifraude'] = np.nan

    if df_antifraude.empty:
        df_antifraude = base_analisar
        df_antifraude['ramificacao_antifraude'] = 'PF FUNDACAO < 2 ANOS'
        df_antifraude['resposta'] = 'REPROVADO'

        print(df_antifraude)
        print(base_analisar)

    else:
        # Criando condições filtro
        condicoes = [
            (df_antifraude['flag_mudanca_estado'] == 'Sim', 'AF - MUDANÇA ESTADO'),
            (df_antifraude['flag_mudanca_cidade'] == 'Sim', 'AF - MUDANÇA CIDADE'),
            (df_antifraude['flag_mudanca_endereco'] == 'Sim', 'AF - MUDANÇA ENDEREÇO'),
            (df_antifraude['flag_endereco_igual'] == 'Sim', 'AF - ENDEREÇO IGUAL')
        ]

        for condition, value in condicoes:
            df_antifraude.loc[condition & df_antifraude['ramificacao_antifraude'].isna(), 'ramificacao_antifraude'] = value
        
        df_antifraude.loc[df_antifraude['ramificacao_antifraude'].isna(), 'ramificacao_antifraude'] = 'AF SEGUE'

        # Criando Resposta
        response_map = {
            'REPROVADO': [ 'AF - MUDANÇA CIDADE', 'AF - MUDANÇA ESTADO'],
            'SEGUE': ['AF SEGUE', 'AF - ENDEREÇO IGUAL', 'AF - MUDANÇA ENDEREÇO']
        }

        # FOI COLOCADO TUDO NO SEGUE, POIS OS CASOS ESTAVAM INDO PARA A MESA SEM COMPRAR O SERASA ATUALIZADO


        print("DF Antifraude:")
        print(df_antifraude)


        # Aplicar as respostas
        for response, values in response_map.items():
            df_antifraude.loc[df_antifraude['ramificacao_antifraude'].isin(values), 'resposta'] = response

        print(df_antifraude)
        print(base_analisar)
    # Juntando bases
    
    df = df_antifraude.merge(base_analisar[['cnpj_sem_formatacao', 'issue_jira', 'inad_alpe', 'pgid', 'limite_solicitado']], on = ['cnpj_sem_formatacao'], how = 'left')

    df['limite_solicitado'] = df['limite_solicitado'].astype(str)

    # Tratando limite_solicitado para task seguinte

    def limpar_simbolo(valor):
        valor = valor.replace('R$', '').strip()

        # Verifica se o valor contém vírgula (formato 'R$ 250.000,99' ou 'R$ 250,000')
        if ',' in valor:
            valor = valor.replace('.', '').replace(',', '.')
        else:
            valor = valor.replace('.', '')  # Remove pontos de milhar, se houver

        return valor

    df['limite_solicitado'] = df['limite_solicitado'].apply(limpar_simbolo)

    df['limite_solicitado'] = df['limite_solicitado'].replace('', '0')

    df['limite_solicitado'] = df['limite_solicitado'].astype(float)

    print(df)

    print(f"Demonstrativo relação pré-filtro: {df.groupby(['issue_jira', 'cnpj_sem_formatacao', 'ramificacao_antifraude'])['cnpj_sem_formatacao'].size()}")

    ### Salvando DF para utilizar na próxima tarefa da DAG
    return df.to_dict(orient='records')
    