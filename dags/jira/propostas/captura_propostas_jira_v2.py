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
from deltalake import write_deltalake


def captura_propostas_jira(access_params=None, **kwargs):


    # Função para buscar a data de mudança de status para "Awaiting Execution"
    def get_data_disponivel(issue_key, jira_url_base, email, api_token, headers):
        changelog_url = f"{jira_url_base}/rest/api/3/issue/{issue_key}?expand=changelog"
        response = requests.get(changelog_url, 
                                headers=headers, 
                                auth=HTTPBasicAuth(email, api_token))
        
        if response.status_code == 200:
            issue_data = response.json()
            changelog = issue_data.get('changelog', {}).get('histories', [])
            
            # Iterar sobre o changelog para encontrar a mudança de status para "Analyzing Credit Score"
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
    jira_url_base  = "https://alpe.atlassian.net"
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
        response = requests.post(jira_url_search, 
                                headers=headers, 
                                auth=HTTPBasicAuth(email, api_token), 
                                data=json.dumps(params))

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

    def process_issue(issue):
        issue_key = issue['key']
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
            'data_criado': issue['fields']['created'],
            'data_resolvido': issue['fields']['resolutiondate'],
            'data_atualizado': issue['fields']['updated']
        }

        # Atraso entre requisições para evitar sobrecarga
        time.sleep(0.4)  # Intervalo de 400ms

        # Adicionando a data de mudança (assumindo que é lenta)
        issue_data['data_disponivel_mesa'] = get_data_disponivel(issue_key, jira_url_base, email, api_token, headers)
        
        return issue_data

    # Processando as issues com informações detalhadas por interação
    start_time = time.time()
    processed_count = 0

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(process_issue, issue) for issue in issues_list]
        total_issues = len(issues_list)
        
        for future in tqdm(as_completed(futures), total=total_issues, desc="Processando issues"):
            processed_count += 1
            issues_data.append(future.result())
            
            if processed_count <= 1000:
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


    df_jira = pd.DataFrame(issues_data)


    print('Tratando JSON do parecer...')


    # Função para extrair o texto
    def extrair_parecer(parecer):
        try:
            return parecer['content'][0]['content'][0]['text']
        except (KeyError, IndexError, TypeError):
            return None

    # Aplicando a função para criar uma nova coluna com o texto extraído
    df_jira['parecer'] = df_jira['parecer'].apply(extrair_parecer)


    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_jira['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_jira['year'], df_jira['month'], df_jira['day'] = now.year, now.month, now.day

    # Reset Index
    df_jira = df_jira.reset_index(True)

    # Exportando dados para a camada Trusted
    # # Conectando na Trusted
    logger = LoggingMixin().log 

    try:
        logger.info("Iniciando salvamento das informações")
        
        storage_options = {
            "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_raw'],
            "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_raw'],
            "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_raw']}",
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
        }

        # Definindo o caminho e salvando no MinIO
        BUCKET_SOURCE_TRUSTED = "jira"

        write_deltalake(
            f"s3a://{BUCKET_SOURCE_TRUSTED}/propostas", 
            df_jira, 
            partition_by=["year", "month", "day"],
            storage_options=storage_options,
            mode="overwrite"
        )
        logger.info("Salvamento concluído com sucesso.")

    except Exception as e:
        logger.error(f"Erro ao salvar as informações: {str(e)}")
