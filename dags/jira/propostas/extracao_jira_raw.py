# Importando Libs
import requests
import pandas as pd
import base64
from minio import Minio
from io import BytesIO
from datetime import datetime, timezone, timedelta
import numpy as np
from requests.auth import HTTPBasicAuth
import json
from airflow.utils.log.logging_mixin import LoggingMixin


def base_details_raw(access_params=None, **kwargs):
    # Configurações da API do Jira
    jira_url_search = "https://alpe.atlassian.net/rest/api/2/search"  # API versão 2 para search
    jira_url_changelog = "https://alpe.atlassian.net/rest/api/3/issue/{issue_id}/changelog"  # API versão 3 para changelog

    # Credenciais de acesso
    email = "felipe.ferraz@alpe.com.br"
    api_token = "ATATT3xFfGF0HVdx6POVBSFWH3BnpC0HyKvTF9EXgjLZ6rpwZuHnIAcI1UDeNTht79mL-O60ezlE4a2qKr4d-H_A3DmY7VCwATPRMABBMQns1ubEjMI_uFgCjEMPeUKkpVgb_-BZ5btV7yQQal1ZNmEm2dGZY_NTpUpOkHdC-SjK0iiBIj3EP80=037ADAA4"

    # Cabeçalhos da requisição
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    # Conectar ao Minio para obter o arquivo Parquet
    client = Minio(
        access_params['endpoint_url_raw'],
        access_key=access_params['aws_access_key_id_raw'],
        secret_key=access_params['aws_secret_access_key_raw']
    )

    # Definindo bucket e caminho do arquivo
    BUCKET_SOURCE_RAW = "jira"
    FOLDER_DESTINATION_RAW = 'propostas'
    file_name = 'base_jira_propostas.parquet'
    file_path = f'{FOLDER_DESTINATION_RAW}/{file_name}'

    # Verificar se o arquivo Parquet já existe no Minio
    file_exists = False
    try:
        response = client.get_object(BUCKET_SOURCE_RAW, file_path)
        file_exists = True
    except:
        file_exists = False

    if file_exists:
        # Se o arquivo existir, carregar o Parquet e proceder com validações
        print("Arquivo existente encontrado. Carregando dados...")

        # Lendo o arquivo da Raw
        file_data = BytesIO(response.read())
        df_existing = pd.read_parquet(file_data)

        # Lista de issue_keys existentes no dataframe
        existing_issue_keys = df_existing['issue_key'].tolist()
        existing_issues_updated = dict(zip(df_existing['issue_key'], df_existing['atualizado']))

    else:
        # Se o arquivo não existir, reprocessar tudo a partir da API
        print("Arquivo não encontrado. Iniciando o processamento das issues...")

        df_existing = pd.DataFrame()  # Criar dataframe vazio, pois não há dados antigos

        # Lista de issue_keys existentes (não há, então é vazio)
        existing_issue_keys = []
        existing_issues_updated = {}

    # Data limite de 15 dias atrás
    fifteen_days_ago = datetime.now() - timedelta(days=70)

    # Query JQL
    print("Listando páginas na API para consolidar em uma lista...")

    jql_query = 'project = cmgt'

    # Parâmetros de paginação
    start_at = 0
    max_results = 100
    total_issues = 0
    issues_list = []

    # Paginar até que todos os dados sejam recuperados
    while True:
        params = {
            "jql": jql_query,
            "startAt": start_at,
            "maxResults": max_results,
            "fields": ["summary", "status", "assignee", "resolution", "created", "customfield_13729", "customfield_13739", "resolutiondate", "customfield_13807", "customfield_13737", "customfield_13709", "customfield_13743",
                    "customfield_13798", "customfield_13793", "customfield_13742", "customfield_13753", "customfield_13721", "priority", "updated"]
        }

        # Requisição para a API do Jira
        response = requests.post(jira_url_search, 
                                 headers=headers, 
                                 auth=HTTPBasicAuth(email, api_token), 
                                 data=json.dumps(params))

        # Verificar se a requisição foi bem-sucedida
        if response.status_code == 200:
            data = response.json()
            issues = data['issues']
            issues_list.extend(issues)
            total_issues += len(issues)
            print(f"Total de issues carregadas até agora: {total_issues}")
            
            if len(issues) == 0:  # Se não houver mais issues a buscar, termina o loop
                break
            start_at += max_results
        else:
            print(f"Erro: {response.status_code}")
            print(response.text)
            break
    
    print("Carregamento finalizado com sucesso!")

    # Coletar dados das issues novas e histórico
    issues_data = []
    status_history = []

    for issue in issues_list:
        issue_key = issue['key']

        # Verificar se a issue já existe nos dados existentes
        if issue_key not in existing_issue_keys:
            # Se a issue for nova, vamos buscar o changelog
            print(f"Issue nova encontrada: {issue_key}. Buscando changelog.")
            changelog_url = jira_url_changelog.format(issue_id=issue_key)
            changelog_response = requests.get(changelog_url, headers=headers, auth=HTTPBasicAuth(email, api_token))

            if changelog_response.status_code == 200:
                changelog_data = changelog_response.json()
                for history in changelog_data.get('values', []):
                    for item in history['items']:
                        if item['field'] == 'status':  # Filtrar mudanças de status
                            status_history.append({
                                'issue_key': issue_key,
                                'status': item.get('toString', None),
                                'datetime_entered': history.get('created', None)
                            })
                print(f"Changelog carregado com sucesso para a issue {issue_key}.")
            else:
                print(f"Erro ao buscar changelog para {issue_key}. Status code: {changelog_response.status_code}")

        else:
            # Se a issue já existe, verifica se há atualização recente (dentro de 15 dias)
            existing_update_time = pd.to_datetime(existing_issues_updated.get(issue_key))  # Tempo de atualização no dataframe
            api_updated_time = pd.to_datetime(issue['fields']['updated'])  # Tempo de atualização da API

            # Garantir que ambas as datas sejam 'naive' (sem fuso horário)
            existing_update_time = existing_update_time.tz_localize(None) if existing_update_time.tzinfo else existing_update_time
            api_updated_time = api_updated_time.tz_localize(None) if api_updated_time.tzinfo else api_updated_time

            # Comparar com a data limite de 15 dias atrás
            if api_updated_time >= fifteen_days_ago or existing_update_time >= fifteen_days_ago:
                # Se a issue foi atualizada nos últimos 15 dias, buscar o changelog
                print(f"Issue existente com atualização recente: {issue_key}. Buscando changelog.")
                changelog_url = jira_url_changelog.format(issue_id=issue_key)
                changelog_response = requests.get(changelog_url, headers=headers, auth=HTTPBasicAuth(email, api_token))

                if changelog_response.status_code == 200:
                    changelog_data = changelog_response.json()
                    for history in changelog_data.get('values', []):
                        for item in history['items']:
                            if item['field'] == 'status':  # Filtrar mudanças de status
                                status_history.append({
                                    'issue_key': issue_key,
                                    'status': item.get('toString', None),
                                    'datetime_entered': history.get('created', None)
                                })
                    print(f"Changelog atualizado com sucesso para a issue {issue_key}.")
                else:
                    print(f"Erro ao buscar changelog para {issue_key}. Status code: {changelog_response.status_code}")
            else:
                print(f"Issue {issue_key} não tem atualizações recentes. Skipping changelog.")

        # Coletar os dados principais da issue
        issue_data = {
            'issue_key': issue_key,
            'politica': issue['fields'].get('customfield_13793', {}).get('value') if issue['fields'].get('customfield_13793') else None,
            'cnpj': issue['fields']['customfield_13729'],
            'pgid': issue['fields']['customfield_13739'],
            'limite_pedido': issue['fields']['customfield_13737'],
            'limite_aprovado': issue['fields']['customfield_13709'],
            'nome_issue': issue['fields']['summary'],
            'nome_vendedor_alpe': issue['fields']['customfield_13743'],
            'nome_vendedor_fn': issue['fields']['customfield_13742'],
            'filial_fn': issue['fields']['customfield_13798'],
            'prioridade': issue['fields'].get('priority', {}).get('name') if issue['fields'].get('priority') else None,
            'status': issue['fields']['status']['name'],
            'decisor': issue['fields']['assignee']['displayName'] if issue['fields']['assignee'] else None,
            'decisao': issue['fields']['resolution']['name'] if issue['fields']['resolution'] else None,
            'parecer': issue['fields']['customfield_13753'],
            'ramificacao_motor': issue['fields']['customfield_13807'],
            'criado': issue['fields']['created'],
            'resolvido': issue['fields']['resolutiondate'],
            'atualizado': issue['fields']['updated']
        }

        issues_data.append(issue_data)

    # Criar DataFrame principal com informações das issues
    df_issues = pd.DataFrame(issues_data)

    # Criar DataFrame de histórico de status
    df_status = pd.DataFrame(status_history)

    df_status['datetime_entered'] = pd.to_datetime(df_status['datetime_entered'], errors='coerce')
    df_status['datetime_entered'] = df_status['datetime_entered'].dt.tz_localize(None)

    df_status = df_status.drop_duplicates(subset=['issue_key', 'status'])

    # Transformar o histórico em formato wide (uma coluna por status)
    df_pivot = df_status.pivot(index='issue_key', columns='status', values='datetime_entered')

    # Combinar os dados principais com o histórico de status
    df_final = df_issues.merge(df_pivot, on='issue_key', how='left')

    # Exibir o resultado
    print(df_final.head())

    # Salvar os dados no Minio (atualizar o arquivo Parquet)
    if file_exists:
        print("Arquivo encontrado, atualizando com as novas issues...")
    else:
        print("Arquivo não encontrado, salvando novo arquivo Parquet...")


    # Salvando Output
    
    logger = LoggingMixin().log 
    
    try:
        logger.info("Iniciando salvamento das informações")

        BUCKET_SOURCE_RAW = "jira"
        FOLDER_DESTINATION_RAW = 'propostas'

        # Conectando na raw
        client = Minio(
            access_params['endpoint_url_raw'],
            access_key=access_params['aws_access_key_id_raw'],
            secret_key=access_params['aws_secret_access_key_raw']
            )


        # Nome do arquivo Parquet que você deseja criar
        file_out = f'base_jira_propostas.parquet'  # Alterando a extensão para .parquet

        # Convertendo o DataFrame para Parquet e armazenando em BytesIO
        parquet_bytes = df_final.to_parquet(index=False)
        parquet_buffer = BytesIO(parquet_bytes)

        # Upload para o MinIO
        client.put_object(
            f'{BUCKET_SOURCE_RAW}',
            f'{FOLDER_DESTINATION_RAW}/{file_out}',
            data=parquet_buffer,
            length=len(parquet_bytes)
        )
    except Exception as e:
        logger.error(f"Erro ao salvar as informações: {str(e)}")