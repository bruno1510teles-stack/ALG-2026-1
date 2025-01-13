# Carregando libs
import pandas as pd
import numpy as np
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import requests, logging, json, time, base64
from jira import JIRA

def analise_pre_filtro(access_params=None,  **kwargs):

    print('Init task')
    ### Configurando configurações necessárias
    # Trino
    conn = connect(
        host=access_params['trino_endpoint'],
        port=access_params['trino_port'],
        user=access_params['trino_user'],
        auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
        http_scheme="https",
    )

    ### Criando uma blacklist para barrar solicitações indesejadas, como por exemplo
    # fast-track para fornecedores da Yandeh
    black_list_pgid = [
        'adoro',
        'aniolli',
        'bariloche',
        'bassar',
        'benassi',
        'caboclo',
        'comprefacil',
        'embala',
        'girotrade',
        'ltcarol',
        'ltdeale',
        'ocean',
        'philipmorris',
        'roge',
        'seugil',
        'ultracheese',
        'yandeh'
    ] 

    ### Validando se a raiz do CNPJ foi analisada a menos de 60 DIAS
    # Configurações da API do Jira
    jira_url = f"{access_params['jira_url']}/rest/api/2/search"
    # Credenciais de acesso
    email = access_params['jira_api_user']
    api_token = access_params['jira_api_token']
    # Gerando o header de autenticação em Base64
    auth = base64.b64encode(f"{email}:{api_token}".encode()).decode()

    jira_url_transition = f"{access_params['jira_url']}"
    jira_connection = JIRA(basic_auth=(email, api_token), server=jira_url_transition)

    headers = {
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/json"
    }   

    max_results = 100  # Defina o número máximo de resultados por página (até 1000 conforme a configuração do Jira)
    start_at = 0       # Inicie na primeira página de resultados
    all_tickets = []   # Lista para armazenar todos os tickets
    data = []   # Data para armazenar os resultados finais
    jira_project = access_params['jira_project']

    query = {
        "jql": f'project = {jira_project} AND status = "Open"',
        "fields": [
            "key",  # ISSUE_JIRA
            "customfield_13808",  # LIMITE ALPE
            "assignee",
            "customfield_13729", # CNPJ do Sacado 
            "customfield_13730", # CNPJ do cedente
            "customfield_13804", # pular pre filtro
            "customfield_13739"], # pgid do cedente 
        "maxResults": max_results,
        "startAt": start_at
    }

    try:   
        response = requests.get(jira_url, headers=headers, params=query)
        response.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        logging.error(f"HTTP Error: {errh}")
    except requests.exceptions.ConnectionError as errc:
        logging.error(f"Error Connecting: {errc}")
    except requests.exceptions.Timeout as errt:
        logging.error(f"Timeout Error: {errt}")
    except requests.exceptions.RequestException as err:
        logging.error(f"General Error: {err}")
    else:
        logging.info("Request was successful.")

    if response.status_code != 200:
        return
    tickets = response.json().get('issues', [])
    # Se não houver mais tickets, parar a paginação
    if not tickets:
        logging.info("Não encontrou propostas em aberto !!")
        return

    for ticket in tickets:

        # Condição para pular o prefiltro

        # Acessando o assignee corretamente dentro de fields
        assignee            = ticket['fields'].get('assignee')
        cpnj_jira           = ticket['fields'].get('customfield_13729')
        assignee_name       = assignee['displayName'] if assignee else 'Não atribuído'
        resolucao           = ticket['fields'].get('resolution', {})
        limite_solicitado   = ticket['fields'].get('customfield_13730')
        key_jira            = ticket['key']
        pular_pre_filtro    = ticket['fields'].get('customfield_13804')
        pgid_cedente        = ticket['fields'].get('customfield_13739')

        if cpnj_jira is None:
            jira_connection.transition_issue(key_jira, "261") # inelegivel
            print(f'Issue {key_jira} esta inelegivel e foi fechada por falta de CNPJ do Sacado!')
            print("")
            continue
        if pgid_cedente in black_list_pgid:
            jira_connection.transition_issue(key_jira, "261") # inelegivel
            print(f'Issue {key_jira} esta inelegivel e foi fechada por estar na lista de fornecedores que não será analisada pela Alpe, por exemplo Yandeh!')
            print("")
            continue
        if pular_pre_filtro is not None and pular_pre_filtro['value'] == 'Não':
            jira_connection.transition_issue(key_jira, "enrich")
            print(f'Issue {key_jira} foi pedido para não executar o filtro e foi para AWAITING ENRICH !')
            print("")
            continue
        
        if resolucao is None:
            decisao = 'NF'
        else:
            decisao = resolucao.get('name', 'NF')
        
        if limite_solicitado is None:
            limite_solicitado = 0
        else:
            try:
                limite_solicitado = float(limite_solicitado.replace('.', '').replace(',', '.').replace("R$", ""))
            except ValueError as er:
                limite_solicitado = 0

        # Adiciona ticket à lista all_tickets
        all_tickets.append(ticket)

        print(f"Issue {key_jira} -> cnpj {cpnj_jira}")

        cnpj_raiz = cpnj_jira[0:8].zfill(8)
        
        query = {
            "jql": f'project = {jira_project} AND resolutiondate >= -60d AND "Payer Identification" ~ "{cnpj_raiz}*" AND key != {key_jira}',
            "fields": [
                "key",  # ISSUE_JIRA
                "customfield_13729", # CNPJ do Sacado
                "resolution"
                ],
            "maxResults": max_results,
            "startAt": start_at
        }

        try:
            response = requests.get(jira_url, headers=headers, params=query)
            response.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            logging.error(f"HTTP Error: {errh}")
        except requests.exceptions.ConnectionError as errc:
            logging.error(f"Error Connecting: {errc}")
        except requests.exceptions.Timeout as errt:
            logging.error(f"Timeout Error: {errt}")
        except requests.exceptions.RequestException as err:
            logging.error(f"General Error: {err}")
        else:
            logging.info("Request was successful.")
        
        response_analise_60_dias = response.json().get('issues', [])
        
        issues_passadas = []

        for issue in response_analise_60_dias:
            # Considerar se existem issues com menos de 60 dias aprovadas para não zerar o limite
            issues_passadas.append({
                "issue": issue['key'],
                "cnpj_raiz": cnpj_raiz,
                "decisao": issue['fields'].get('resolution')['name']
            })

        issues_passadas_df = pd.DataFrame(issues_passadas)
        analise_menor_60_dias = False
        if not issues_passadas_df.empty:
            issues_passadas_df.sort_values([
                'cnpj_raiz', 'decisao'
            ])
            issues_passadas_df = issues_passadas_df.drop_duplicates(subset='cnpj_raiz')
            
            decisao = issues_passadas_df['decisao'].values[0]
            analise_menor_60_dias = not issues_passadas_df.empty

            print("Issue analisada a menos de 60 dias.")
            print(issues_passadas_df)
            print("")
        else:
            print(f"Sem issues analisadas a menos de 60 dias para {cnpj_raiz}.")
            print("")

        # Adiciona dados ao DataFrame
        data.append({
            'cnpj_raiz': cnpj_raiz,
            'cnpj_jira': cpnj_jira,
            'analise_menor_60_dias': analise_menor_60_dias,
            'decisor': assignee_name,
            'decisao': decisao,
            'limite_solicitado': limite_solicitado,
            'issue_jira': key_jira
        })

    # Criando um DataFrame com os resultados
    df_jira = pd.DataFrame(data)

    if df_jira.empty:
        return
    
    print("Issues selecionadas ...")
    print(df_jira)
    print("")

    ### Funções SQL
    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        return pd.DataFrame(rows, columns=columns)

    ids_query = ', '.join(f"'{df['cnpj_raiz']}'" for df in data)
    print("")
    print(f"Cnpjs Raiz para buscar na base da RF: {ids_query}")
    print("")
    ids_query = f"({ids_query})"
    
    if ids_query == "()":
        ids_query = "('')"

    # Base estabelecimentos
    query_estabelecimentos = f"""
        SELECT DISTINCT est.documento_sem_formatacao 
        FROM deltalaketrusted.receita_federal.estabelecimentos est
        WHERE est.cnpj_raiz IN {ids_query} AND is_matriz = true
    """
    base_analisar_raiz = execute_query(conn, query_estabelecimentos)

    ## Criar uma string formatada para a cláusula IN
    base_analisar_raiz['cnpj_raiz'] = base_analisar_raiz['documento_sem_formatacao'].str.slice(0, 8).str.zfill(8)

    df = pd.DataFrame()

    # Cria um cursor e executa a query
    cur = conn.cursor()

    for index, row in base_analisar_raiz.iterrows():
        cnpj_completo = row['documento_sem_formatacao']
        cnpj_raiz = row['cnpj_raiz'] 

        # Marca o tempo de início
        start_time = time.time()

        query = (f"""

            with limite as (
                select 
                    cnpj_raiz, case when limite_atribuido > 0 then true else false end as limite_alpe, NULLIF(limite_disponivel, 0) / NULLIF(limite_atribuido, 0) AS pcto_limite_utilizado, situacao_sacado, limite_atribuido
                from (
                    select 
                        pc.chave as cnpj_raiz,
                        lc.id is not null as limite_alpe,
                        sum(limite_atribuido) as limite_atribuido,
                        sum(limite_disponivel) as limite_disponivel,
                        pl.status as situacao_sacado
                    from postgres.ccred_schema_prd_default.participante_chave pc
                    inner join postgres.ccred_schema_prd_default.limite_config lc on lc.participante_chave_sacado_id = pc.id
                    inner join postgres.ccred_schema_prd_default.participante_limite pl on pl.limite_config_id = lc.id      
                    where
                    pc.chave = COALESCE('{cnpj_raiz}', '')
                    group by
                        pc.chave, lc.id is not null, pl.status)
                            )                    
            select 
                pre.cnpj_raiz cnpj_raiz,
                pre.documento_sem_formatacao,
                pre.situacao_cadastral situacao_cadastral,
                coalesce(lim.situacao_sacado, 'ATIVO') as situacao_sacado,
                coalesce(lim.limite_atribuido, 0) as limite_atribuido,
                coalesce(lim.pcto_limite_utilizado, 0) as pcto_limite_utilizado
            from 
                deltalakerefined.motor.pre_filtro pre
            left join limite lim on lim.cnpj_raiz = pre.cnpj_raiz
            where pre.documento_sem_formatacao =  COALESCE('{cnpj_completo}', '')

            """)

        # Executa a query
        cur.execute(query)

        # Obtém os resultados
        rows = cur.fetchall()

        # Para pegar o nome das colunas
        columns = [desc[0] for desc in cur.description]

        # Converte os resultados em um DataFrame temporário
        df_temp = pd.DataFrame(rows, columns=columns)

        # Concatenar os resultados temporários no DataFrame final
        df = pd.concat([df, df_temp], ignore_index=True)

        # Marca o tempo de fim e calcula a duração
        end_time = time.time()
        elapsed_time = end_time - start_time

        # Exibe o tempo de execução para cada CNPJ
        print(f"Tempo de execução para CNPJ {cnpj_completo}: {elapsed_time:.2f} segundos")

    # Fecha o cursor e a conexão
    cur.close()
    conn.close()
    print("")

    ### Concatenando base principal(import)
    df = df_jira.merge(df[['cnpj_raiz', 'documento_sem_formatacao', 'situacao_cadastral', 'situacao_sacado', 'limite_atribuido', 'pcto_limite_utilizado']], on = ['cnpj_raiz'], how = 'left')

    df.loc[df['situacao_cadastral'].isna(), 'situacao_cadastral'] = 'NAO ENCONTRADO'
    df.loc[df['situacao_sacado'].isna(), 'situacao_sacado'] = 'NAO ENCONTRADO'
    df.loc[df['limite_atribuido'].isna(), 'limite_atribuido'] = 0
    df.loc[df['pcto_limite_utilizado'].isna(), 'pcto_limite_utilizado'] = 0

    pd.set_option('display.max_rows', None)  # Mostra todas as linhas
    pd.set_option('display.max_columns', None)  # Mostra todas as colunas
    pd.set_option('display.width', None)  # Ajusta a largura para que o DataFrame não quebre em várias linhas
    pd.set_option('display.max_colwidth', None)  # Permite exibir o conteúdo completo de cada coluna

    df['ramificacao_pre_filtro'] = np.nan
    print(df)
    # Dicionário para mapear condições a valores de 'ramificacao_pre_filtro'
    conditions = [
        # Impedidos de Operar
        (df['situacao_cadastral'] != 'ATIVA', 'PF CNPJ IRREGULAR'),

        # Filtro Operacional
        ((df['analise_menor_60_dias'] == True) & (df['decisao'] == 'Reproved'), 'PF REPROVA < 60 DIAS'),
        ((df['analise_menor_60_dias'] == True) & (df['decisao'] == 'Approved') & ((df['limite_solicitado'] * 1.2)  <= df['limite_atribuido']) & pd.notna(df['limite_atribuido']), 'PF LIMITE SOLICITADO <= ATUAL'),
        ((df['analise_menor_60_dias'] == True) & (df['decisao'] == 'Approved') & (df['limite_solicitado'] * 1.2 > df['limite_atribuido']) & (df['pcto_limite_utilizado'] < 0.7) & pd.notna(df['pcto_limite_utilizado']), 'PF - UTILIZAÇÃO DE LIMITE MÍNIMA NÃO ATINGIDA'),
        ((df['analise_menor_60_dias'] == True) & (df['decisao'] == 'Duplicado'), 'PN DUPLICADA')
    ]
    # Aplicar condições
    for condition, value in conditions:
        df.loc[condition & df['ramificacao_pre_filtro'].isna(), 'ramificacao_pre_filtro'] = value

    # Atribuir 'PF 12' para os que não se encaixam em nenhuma das condições anteriores
    df.loc[df['ramificacao_pre_filtro'].isna(), 'ramificacao_pre_filtro'] = 'PF SEGUE'

    # Criando Resposta
    response_map = {
        'REPROVADO': ['PF CNPJ IRREGULAR', 'PF REPROVA < 60 DIAS'],
        'mantido' : ['PF LIMITE SOLICITADO <= ATUAL', 'PF - UTILIZAÇÃO DE LIMITE MÍNIMA NÃO ATINGIDA', 'SEM PLEITO'],
        'DUPLICADO': ['PN DUPLICADA']
    }
    # Aplicar as respostas
    for response, values in response_map.items():
        df.loc[df['ramificacao_pre_filtro'].isin(values), 'resposta'] = response

    # Printando resultado
    result = df.groupby(['issue_jira', 'documento_sem_formatacao', 'ramificacao_pre_filtro'])['documento_sem_formatacao'].size()
    print(f"Demonstrativo relação pré-filtro: {result}")

    pd.reset_option('display.max_rows')
    pd.reset_option('display.max_columns')
    pd.reset_option('display.width')
    pd.reset_option('display.max_colwidth')

    for index, iss in df.iterrows():
        
        print(iss)
        #  Adicionar a resolution
        if iss.resposta == "REPROVADO":
            jira_connection.transition_issue(iss.issue_jira, "261") # inelegivel
            print(f'Issue {iss.issue_jira} esta inelegivel e foi fechada !')
            print("")
        elif iss.resposta == "mantido":
            jira_connection.transition_issue(iss.issue_jira, "261") # inelegivel
            print(f'Issue {iss.issue_jira} esta inelegivel e foi fechada !')
            print("")
        elif iss.resposta == 'DUPLICADO':
            jira_connection.transition_issue(iss.issue_jira, "411") # duplicado
            print(f'Issue {iss.issue_jira} esta duplicada e foi fechada !')
            print("")
        else:
            jira_connection.transition_issue(iss.issue_jira, "enrich")
            print(f'Issue {iss.issue_jira} foi para AWAITING ENRICH !')
            print("")
            

    ### Salvando DF para utilizar na próxima tarefa da DAG
    return df.to_dict(orient='records')