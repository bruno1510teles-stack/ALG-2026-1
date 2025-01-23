# Importando Libs
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import sys
from tqdm import tqdm
from airflow.utils.log.logging_mixin import LoggingMixin
from minio import Minio
from io import BytesIO
from datetime import datetime, timedelta, timezone
from logging import Logger


def captura_propostas_jira(access_params=None, **kwargs):


    # Função para buscar a data de mudança de status para "Awaiting Execution"
    def get_data_disponivel(issue_key, jira_url_base, email, api_token, headers):
        changelog_url = f"{jira_url_base}/rest/api/3/issue/{issue_key}?expand=changelog"
        response = requests.get(changelog_url, headers=headers, auth=HTTPBasicAuth(email, api_token))
        
        if response.status_code == 200:
            issue_data = response.json()

            changelog = issue_data.get('changelog', {}).get('histories', [])
            
            # Iterar sobre o changelog para encontrar a mudança de status para "Awaiting Execution"
            for history in changelog:
                for item in history.get('items', []):
                    if item.get('field') == 'status':
                        to_status = item.get('toString')
                        
                        if to_status == "Awaiting Execution":
                            # Retorna a data e hora quando a mudança foi feita
                            return history.get('created')
        else:
            print(f"Erro ao buscar o changelog da issue {issue_key}: {response.status_code}")
        
        return None

    # Configurações da API do Jira
    jira_url_base = "https://alpe.atlassian.net"
    jira_url_search = f"{jira_url_base}/rest/api/3/search"
    # Credenciais de acesso
    email = "felipe.ferraz@alpe.com.br"
    api_token = "ATATT3xFfGF0HVdx6POVBSFWH3BnpC0HyKvTF9EXgjLZ6rpwZuHnIAcI1UDeNTht79mL-O60ezlE4a2qKr4d-H_A3DmY7VCwATPRMABBMQns1ubEjMI_uFgCjEMPeUKkpVgb_-BZ5btV7yQQal1ZNmEm2dGZY_NTpUpOkHdC-SjK0iiBIj3EP80=037ADAA4"

    # Headers para requisições
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    # JQL query e parâmetros de paginação
    jql_query = 'project = cmgt' # and issue = "CMGT-49482"
    start_at = 0
    max_results = 100
    total_issues = 0
    issues_list = []

    # Paginação para carregar todas as issues
    while True:
        params = {
            "jql": jql_query,
            "startAt": start_at,
            "maxResults": max_results,
            "fields": ["summary", "status", "assignee", "resolution", "created", "customfield_13729", "customfield_13739", "resolutiondate", "customfield_13807", "customfield_13737", "customfield_13709", "customfield_13743",
                        "customfield_13798", "customfield_13793", "customfield_13742", "customfield_13753", "customfield_13721", "priority", "updated"]
        }

        # Requisição para API do Jira
        response = requests.post(jira_url_search, headers=headers, auth=HTTPBasicAuth(email, api_token), data=json.dumps(params))

        # Verifica se a requisição foi bem-sucedida
        if response.status_code == 200:
            data = response.json()
            issues = data['issues']
            issues_list.extend(issues)
            total_issues += len(issues)
            print(f"Total de issues carregadas até agora: {total_issues}")
            
            if len(issues) == 0:
                break
            
            start_at += max_results
        else:
            print(f"Erro: {response.status_code}")
            print(response.text)
            break

    issues_data = []

    # Função para processar cada issue
    def process_issue(issue):
        issue_key = issue['key']
        issue_data = {
            'issue_key': issue_key,
            'politica': issue['fields'].get('customfield_13793', {}).get('value') if issue['fields'].get('customfield_13793') else None,
            'cnpj': issue['fields'].get('customfield_13729', None),
            'pgid': issue['fields'].get('customfield_13739', None),
            'limite_pedido': issue['fields'].get('customfield_13737', None),
            'limite_aprovado': issue['fields'].get('customfield_13709', None),
            'nome_issue': issue['fields'].get('summary', None),
            'nome_vendedor_alpe': issue['fields'].get('customfield_13743', None),
            'nome_vendedor_fn': issue['fields'].get('customfield_13742', None),
            'filial_fn': issue['fields'].get('customfield_13798', None),
            'prioridade': issue['fields'].get('priority', {}).get('name') if issue['fields'].get('priority') else None,
            'status': issue['fields'].get('status', {}).get('name', None),
            'decisor': issue['fields'].get('assignee', {}).get('displayName') if issue['fields'].get('assignee') else None,
            'decisao': issue['fields'].get('resolution', {}).get('name') if issue['fields'].get('resolution') else None,
            'parecer': issue['fields'].get('customfield_13753', None),
            'ramificacao_motor': issue['fields'].get('customfield_13807', None),
            'data_criado': issue['fields'].get('created', None),
            'data_resolvido': issue['fields'].get('resolutiondate', None),
            'data_atualizado': issue['fields'].get('updated', None)
        }

        # Atraso entre requisições para evitar sobrecarga
        time.sleep(0.2)  # Intervalo de 300ms

        # Adicionando a data de mudança (assumindo que é lenta)
        data_disponivel = get_data_disponivel(issue_key, jira_url_base, email, api_token, headers)
        issue_data['data_disponivel_mesa'] = data_disponivel if data_disponivel else None

        return issue_data

    # Processando as issues com informações detalhadas por interação
    start_time = time.time()
    processed_count = 0

    with ThreadPoolExecutor(max_workers=7) as executor:
        futures = [executor.submit(process_issue, issue) for issue in issues_list]
        total_issues = len(issues_list)
        
        for future in tqdm(as_completed(futures), total=total_issues, desc="Processando issues"):
            try:
                result = future.result()  # Pode levantar exceções aqui
                issues_data.append(result)
            except Exception as e:
                print(f"Erro ao processar a issue: {e}")
                continue  # Continua processando as outras issues

            # Exibindo informações sobre o progresso
            processed_count += 1
            if processed_count <= 50000:
                elapsed_time = time.time() - start_time
                avg_time_per_issue = elapsed_time / processed_count
                remaining_issues = total_issues - processed_count
                estimated_time_remaining = remaining_issues * avg_time_per_issue
                
                print(f"Processadas: {processed_count}/{total_issues} | "
                    f"Tempo médio por issue: {avg_time_per_issue:.2f}s | "
                    f"Tempo restante estimado: {estimated_time_remaining:.2f}s", end='\r')  # Sobrescreve a linha

                sys.stdout.flush()  # Força a saída do buffer para garantir que o print apareça em tempo real

    end_time = time.time()
    total_time = end_time - start_time
    print(f"\nProcessamento concluído em {total_time:.2f} segundos.")

    # Convertendo os dados para DataFrame
    df_jira = pd.DataFrame(issues_data)

    # Tratando JSON do parecer...
    def extrair_parecer(parecer):
        try:
            return parecer['content'][0]['content'][0]['text']
        except (KeyError, IndexError, TypeError):
            return None

    # Aplicando a função para criar uma nova coluna com o texto extraído
    df_jira['parecer'] = df_jira['parecer'].apply(extrair_parecer)

    # Atribuindo data atual
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_jira['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_jira['year'], df_jira['month'], df_jira['day'] = now.year, now.month, now.day

    # Resetando o índice
    df_jira = df_jira.reset_index(drop=True)


    # Salvando Output

    logger = LoggingMixin().log

    try:
        logger.info("Iniciando salvamento das informações")

        BUCKET_SOURCE_RAW = "jira"
        FOLDER_DESTINATION_RAW = 'propostas'

        # Conectando no MinIO
        client = Minio(
            "api-raw.alpe.com.br",
            access_key = 'B7q0avvSIpSdyGPXWnEC',
            secret_key = 'PhMhRQSQ6YJU8fn2qKhDLM017cQPrlCz1YbM8IwU'
        )

        # Gerando o nome do arquivo com a data atual
        current_date = datetime.now().strftime('%Y-%m-%d')  # Formato de data: '2025-01-21'
        file_out = f'base_jira_propostas_{current_date}.csv'  # Adicionando a data no nome do arquivo

        # Convertendo o DataFrame para CSV e armazenando em BytesIO
        csv_bytes = df_jira.to_csv(index=False).encode()  # Convertendo para CSV (e encode para bytes)
        csv_buffer = BytesIO(csv_bytes)

        # Upload para o MinIO
        client.put_object(
            BUCKET_SOURCE_RAW,
            f'{FOLDER_DESTINATION_RAW}/{file_out}',
            data=csv_buffer,
            length=len(csv_bytes)
        )

        logger.info(f"Arquivo {file_out} salvo com sucesso no MinIO.")

    except Exception as e:
        logger.error(f"Erro ao salvar as informações: {str(e)}")