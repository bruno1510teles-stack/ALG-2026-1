# Importando Libs
import requests
import pandas as pd
import base64
from minio import Minio
from io import BytesIO
import numpy as np
from requests.auth import HTTPBasicAuth
import json



def base_details_raw(access_params=None):
           
    # Configurações da API do Jira
    jira_url = "https://alpe.atlassian.net/rest/api/2/search"
    # Credenciais de acesso
    email = "felipe.ferraz@alpe.com.br"
    api_token = "ATATT3xFfGF0HVdx6POVBSFWH3BnpC0HyKvTF9EXgjLZ6rpwZuHnIAcI1UDeNTht79mL-O60ezlE4a2qKr4d-H_A3DmY7VCwATPRMABBMQns1ubEjMI_uFgCjEMPeUKkpVgb_-BZ5btV7yQQal1ZNmEm2dGZY_NTpUpOkHdC-SjK0iiBIj3EP80=037ADAA4"
    # Gerando o header de autenticação em Base64
    auth = base64.b64encode(f"{email}:{api_token}".encode()).decode()


    # Cabeçalhos da requisição
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
        }

    # Query JQL
    jql_query = 'project = cmgt'

    # Parâmetros de paginação
    start_at = 0
    max_results = 100  # Teste com 100, pois pode ser o limite do seu servidor
    total_issues = 0
    issues_list = []  # Lista para armazenar os resultados

    # Paginar até que todos os dados sejam recuperados
    while True:
        # Parâmetros da requisição, incluindo paginação
        params = {
            "jql": jql_query,
            "startAt": start_at,
            "maxResults": max_results,
            "fields": ["summary", "status", "assignee", "resolution", "created", "customfield_13729", "customfield_13739", "resolutiondate", "customfield_13807", "customfield_13737", "customfield_13709", "customfield_13743",
                    "customfield_13798", "customfield_13793", "customfield_13742", "customfield_13753", "customfield_13721"]  # Campos que deseja extrair
        }
        
        # Requisição para a API do Jira
        response = requests.post(jira_url, 
                                headers=headers, 
                                auth=HTTPBasicAuth(email, api_token), 
                                data=json.dumps(params))
        
        # Verificar se a requisição foi bem-sucedida
        if response.status_code == 200:
            data = response.json()
            issues = data['issues']
            issues_list.extend(issues)  # Adicionar os novos issues à lista
            total_issues += len(issues)
            print(f"Total de issues carregadas até agora: {total_issues}")
            
            # Se o número de issues retornadas for menor que o max_results, significa que terminamos
            if len(issues) == 0:  # Se não houver mais issues a buscar, termina o loop
                break
            
            # Atualizar o startAt para a próxima página
            start_at += max_results
        else:
            print(f"Erro: {response.status_code}")
            print(response.text)
            break

    # Converter a lista de issues para um DataFrame pandas
    issues_data = []

    # Função para trazer epochMillis
    def get_epoch_millis(issue):
        completed_cycles = issue['fields'].get('customfield_13721', {}).get('completedCycles', [])
        
        # Verifica se existe pelo menos um ciclo completo
        if completed_cycles and 'startTime' in completed_cycles[0]:
            return completed_cycles[0]['startTime'].get('epochMillis', None)
        
        return None
            
    for issue in issues_list:
                
        
        issues_data.append({
            'issue_key': issue['key'],
            'politica': issue['fields'].get('customfield_13793', {}).get('value') if issue['fields'].get('customfield_13793') else None,
            'cnpj': issue['fields']['customfield_13729'],
            'pgid': issue['fields']['customfield_13739'],
            'limite_pedido': issue['fields']['customfield_13737'],
            'limite_aprovado': issue['fields']['customfield_13709'],
            'nome_issue': issue['fields']['summary'],
            'nome_vendedor_alpe': issue['fields']['customfield_13743'],
            'nome_vendedor_fn': issue['fields']['customfield_13742'],
            'filial_fn': issue['fields']['customfield_13798'],
            'status': issue['fields']['status']['name'],
            'decisor': issue['fields']['assignee']['displayName'] if issue['fields']['assignee'] else None,
            'decisao': issue['fields']['resolution']['name'] if issue['fields']['resolution'] else None,
            'parecer': issue['fields']['customfield_13753'],
            'ramificacao_motor': issue['fields']['customfield_13807'],
            'criado': issue['fields']['created'],
            'resolvido': issue['fields']['resolutiondate'],
            'dataatribuido': get_epoch_millis(issue)
        })

    # Criar o DataFrame
    df = pd.DataFrame(issues_data)

        # Exibir DataFrame
    print(df)



    # Salvando Output

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
    parquet_bytes = df.to_parquet(index=False)
    parquet_buffer = BytesIO(parquet_bytes)

    # Upload para o MinIO
    client.put_object(
        f'{BUCKET_SOURCE_RAW}',
        f'{FOLDER_DESTINATION_RAW}/{file_out}',
        data=parquet_buffer,
        length=len(parquet_bytes)
    )