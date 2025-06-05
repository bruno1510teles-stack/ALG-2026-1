# Importando Libs
import requests
import pandas as pd
import base64
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from deltalake import write_deltalake
from datetime import datetime, timezone, timedelta
import numpy as np
import logging
from io import BytesIO
import re

def captura_proposta(access_params=None):

    # Configurações da API do Jira
    #jira_url = f"{access_params['jira_url']}/rest/api/2/search"
    jira_url = "https://alpe.atlassian.net/rest/api/2/search"
    # Credenciais de acesso
    #email = access_params['jira_user']
    
    email = "felipe.ferraz@alpe.com.br"
    #api_token = access_params['jira_token']
    api_token = "ATATT3xFfGF0HVdx6POVBSFWH3BnpC0HyKvTF9EXgjLZ6rpwZuHnIAcI1UDeNTht79mL-O60ezlE4a2qKr4d-H_A3DmY7VCwATPRMABBMQns1ubEjMI_uFgCjEMPeUKkpVgb_-BZ5btV7yQQal1ZNmEm2dGZY_NTpUpOkHdC-SjK0iiBIj3EP80=037ADAA4"

    # Gerando o header de autenticação em Base64
    auth = base64.b64encode(f"{email}:{api_token}".encode()).decode()

    headers = {
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/json"
    }
    max_results = 100  # Defina o número máximo de resultados por página (até 1000 conforme a configuração do Jira)
    start_at = 0       # Inicie na primeira página de resultados
    all_tickets = []   # Lista para armazenar todos os tickets

    while True:
        # Query para buscar os tickets da fila desejada
        query = {
            "jql": "project = cmgt AND Política = 'Política 4' AND status = 'Analyzing Credit Score'",
            "fields": ["key",  # ISSUE_JIRA
                    "summary",
                    "customfield_13729",  # CNPJ
                    "customfield_13732",  # LIMITE ALPE
                    "customfield_13808",  # INAD ALPE 
                    "customfield_13739",  # PGID FN
                    "customfield_13719", # NOME PGID FN
                    "customfield_13737"],  # LIMITE SOLICITADO
            "maxResults": max_results,
            "startAt": start_at
        }

        # Fazendo a requisição para o Jira
        response = requests.get(jira_url, headers=headers, params=query)

        if response.status_code == 200:
            # Processando a resposta
            tickets = response.json()['issues']
            
            if not tickets:
                # Se não houver mais tickets, parar a paginação
                break

            all_tickets.extend(tickets)  # Adiciona os tickets retornados à lista geral
            
            # Atualiza o ponto inicial para a próxima página
            start_at += max_results
        else:
            print(f"Failed to fetch data from Jira: {response.status_code}")
            break

    # Extraindo os valores dos campos
    data = []
    for ticket in all_tickets:
        issue_jira = ticket.get('key')  
        summary = ticket['fields'].get('summary')
        cnpj = ticket['fields'].get('customfield_13729')  
        limite_alpe = ticket['fields'].get('customfield_13732')  
        inad_alpe = ticket['fields'].get('customfield_13808')  
        pgid = ticket['fields'].get('customfield_13739')  
        nome_pgid = ticket['fields'].get('customfield_13719')
        limite_solicitado = ticket['fields'].get('customfield_13737')
        
        data.append({
            'issue_jira': issue_jira,
            'CNPJ': cnpj,
            'limite_alpe': limite_alpe,
            'inad_alpe': inad_alpe,
            'pgid' : pgid,
            'nome_pgid': nome_pgid,
            'nome_issue': summary,
            'limite_solicitado' : limite_solicitado
        })

    # Criando um DataFrame com os CNPJs
    df = pd.DataFrame(data)
    print(f"Quantidade de CNPJs na fila política v4: {df.shape[0]}")
    print(f"Quantidade de CNPJs na fila aberto por fornecedor: {df.groupby('nome_pgid')['CNPJ'].size()}")


    # # Passo 1: Remover a máscara de valor e converter para numérico
    # if df['limite_alpe'].notna().any():
    #     df['limite_alpe'] = df['limite_alpe'].replace({'R\$ ': '', '\.': ''}, regex=True)
    #     df['limite_alpe'] = pd.to_numeric(df['limite_alpe'], errors='coerce')  

    if df['inad_alpe'].notna().any():
        df['inad_alpe'] = df['inad_alpe'].replace({'R\$ ': '', '\.': ''}, regex=True)
        df['inad_alpe'] = pd.to_numeric(df['inad_alpe'], errors='coerce')  # Converte para float, substituindo erros por NaN

    # Passo 3: Criar as colunas de flag com base na lógica fornecida
    # df['limite_alpe_flag'] = df['limite_alpe'].apply(lambda x: x > 0 if pd.notna(x) else False)
    df['inad_alpe_flag'] = df['inad_alpe'].apply(lambda x: 'SIM' if pd.notna(x) and x > 0 else 'NAO')

    # Passo 4: Deixar o nome padrão
    # Remover as colunas originais 'limite_alpe' e 'inad_alpe'
    #df.drop(columns=['limite_alpe', 'inad_alpe'], inplace=True)
    df.drop(columns=['inad_alpe'], inplace=True)

    # Renomear as colunas de flag para os nomes originais
    #df.rename(columns={'limite_alpe_flag': 'limite_alpe', 'inad_alpe_flag': 'inad_alpe'}, inplace=True)
    df.rename(columns={'inad_alpe_flag': 'inad_alpe'}, inplace=True)

    # Filtrando somente lote
    #df = df[~df['nome_issue'].str.contains('LOTE', case=False, na=False)]

    # Exibir o DataFrame resultante
    print(df.head(10))

    df['cnpj_sacado_raiz'] = df['CNPJ'].str[:8]

    # Extraindo os CNPJs do DataFrame 'limites' e convertendo-os para uma lista
    cnpjs = df['cnpj_sacado_raiz'].unique().tolist()
    # Convertendo a lista para uma string no formato adequado para o SQL
    cnpjs_str = ', '.join([f"'{cnpj}'" for cnpj in cnpjs])

    # Conectando com o Trino
    conn = connect(
        host=access_params['trino_endpoint'],
        port=access_params['trino_port'],
        user=access_params['trino_user'],
        auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
        http_scheme="https",
    )

    # Função para execução da query
    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        return pd.DataFrame(rows, columns=columns)
    
    # Query Situação Receita
    query_receita =  f""" 
                    select distinct 
                        cnpj_raiz as cnpj_sacado_raiz,
                        situacao_cadastral
                    from deltalaketrusted.receita_federal.estabelecimentos
                    where is_matriz = true
                    and cnpj_raiz in ({cnpjs_str})
                    """

    receita = execute_query(conn, query_receita)


    # Query PEP e RJ
    query_pep_rj =  f""" 
                    select 
                        cnpj_raiz as cnpj_sacado_raiz,
                        max(situacao_especial) as situacao_especial,
                        max(tem_pep) as tem_pep
                    from deltalakerefined.motor.pre_filtro
                    where situacao_especial = 'RECUPERACAO JUDICIAL'
                    and cnpj_raiz in ({cnpjs_str})
                    or tem_pep = true
                    group by cnpj_raiz
                    """

    pep_rj = execute_query(conn, query_pep_rj)

    # Cruzando os DFs
    df = pd.merge(df, receita, on='cnpj_sacado_raiz', how='left')

    df = pd.merge(df, pep_rj, on='cnpj_sacado_raiz', how='left')

    # Função para criar o parecer da proposta
    def definir_motivo(row):
        if row['situacao_cadastral'] != 'ATIVA':
            return row['situacao_cadastral']
        elif row['situacao_especial'] == 'RECUPERACAO JUDICIAL':
            return row['situacao_especial']
        elif row['tem_pep'] == True:
            return 'PEP'
        else:
            return 'NAO ENCONTRADO'

    # Criando a nova coluna 'motivo'
    df['parecer'] = df.apply(definir_motivo, axis=1)

    # Buscando os nomes dos socios PEP
    # Query Socios
    query_socios =  f""" 
                    select 
                        "nome/razao_social" as nome_socio_receita,
                        cnpj_raiz as cnpj_sacado_raiz,
                        documento_socio as documento
                    from deltalaketrusted.receita_federal.socios
                    where cnpj_raiz in ({cnpjs_str})
                    """

    socios = execute_query(conn, query_socios)

    # Query Socios PEP
    query_socios_pep =  f""" 
                        select distinct
                            nome as nome_socio,
                            REPLACE(REPLACE(documento, '.', ''), '-', '') as documento,
                            'PEP' as flag_pep
                        from deltalaketrusted.pessoas_e_organizacoes.pep
                        """

    socios_pep = execute_query(conn, query_socios_pep)

    socios_pep_final = pd.merge(socios, socios_pep, on='documento', how='left')

    socios_pep_final['flag_pep'] = socios_pep_final['flag_pep'].fillna(' ')


    # Definindo apenas um socio PEP para ser referenciado no parecer (MAX do VARCHAR)
    df_pep = df[df['parecer'] == 'PEP']

    df_pep_merge = pd.merge(df_pep, socios_pep_final[socios_pep_final['flag_pep'] != ' '], on='cnpj_sacado_raiz', how='left')

    df_pep_merge = df_pep_merge.groupby('cnpj_sacado_raiz')['nome_socio'].max().reset_index()


    # Cruzando com a base de propostas
    df = pd.merge(df, df_pep_merge, on='cnpj_sacado_raiz', how='left')

    df['nome_socio'] = df['nome_socio'].fillna(' ')


    # Dropando colunas desnecessarias e deixando o parecer pronto
    df.loc[df['parecer'] == 'PEP', 'parecer'] = df['parecer'] + " - " + df['nome_socio']

    df = df.drop(['cnpj_sacado_raiz', 'situacao_cadastral', 'situacao_especial', 'tem_pep', 'nome_socio'], axis=1)

    ### Salvando DF para utilizar na próxima tarefa da DAG
    return df.to_dict(orient='records')
