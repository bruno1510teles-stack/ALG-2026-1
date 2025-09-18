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
import datetime


def captura_proposta (access_params = None):


    #Pegando ARQUIVO HISTÓRICO

    # Conectando no MinIO
    client = Minio(
        access_params['endpoint_url_raw'],
        access_key=access_params['aws_access_key_id_raw'],
        secret_key=access_params['aws_secret_access_key_raw'],
    )

    # Definindo bucket e caminho do arquivo
    BUCKET_SOURCE_RAW = "jira"
    FOLDER_DESTINATION_RAW = 'propostas'
    file_name = 'base_jira_propostas.parquet'
    file_path = f'{FOLDER_DESTINATION_RAW}/{file_name}'


    # Lendo o arquivo da Raw
    response = client.get_object(BUCKET_SOURCE_RAW, file_path)
    file_data = BytesIO(response.read())
    df_historico_propostas = pd.read_parquet(file_data)

    # Excluindo colunas de datas, ja que vamos concatenar com as novas propostas e teremos novas datas ref
    df_historico_propostas = df_historico_propostas.drop(columns=['atualizado_em', 'year', 'month', 'day'])



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
    jira_url_search = f"{jira_url_base}/rest/api/3/search/jql"

    # Credenciais de acesso
    email = "felipe.ferraz@alpe.com.br"
    api_token = "ATATT3xFfGF0HVdx6POVBSFWH3BnpC0HyKvTF9EXgjLZ6rpwZuHnIAcI1UDeNTht79mL-O60ezlE4a2qKr4d-H_A3DmY7VCwATPRMABBMQns1ubEjMI_uFgCjEMPeUKkpVgb_-BZ5btV7yQQal1ZNmEm2dGZY_NTpUpOkHdC-SjK0iiBIj3EP80=037ADAA4"

    # Headers para requisições
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }


    def get_ultimo_dia_util():
        hoje = datetime.datetime.now()
        dia = hoje - timedelta(days=1)

        # Volta até encontrar um dia de semana (segunda a sexta)
        while dia.weekday() >= 5:  # 5 = sábado, 6 = domingo
            dia -= timedelta(days=1)

        return dia.strftime('%Y-%m-%d')

    ultimo_dia_util = get_ultimo_dia_util()


    jql_query = f'project = cmgt AND updated >= "{ultimo_dia_util}"'
    max_results = 100
    issues_list = []
    next_token = None  # Inicializa o token como None

    while True:
        params = {
            "jql": jql_query,
            "maxResults": max_results,
            "fields": ["summary", "status", "assignee", "resolution", "created",
                    "customfield_13729", "customfield_13739", "resolutiondate",
                    "customfield_13807", "customfield_13737", "customfield_13709",
                    "customfield_13743", "customfield_13798", "customfield_13793",
                    "customfield_13811", "customfield_13742", "customfield_13753",
                    "customfield_13721", "priority", "updated"]
        }

        if next_token:  # Se já tiver token da última página
            params["nextPageToken"] = next_token

        response = requests.post(jira_url_search, headers=headers,
                                auth=HTTPBasicAuth(email, api_token),
                                json=params)  # usar json=params, não data

        if response.status_code == 200:
            data = response.json()
            issues = data.get("issues", [])
            issues_list.extend(issues)
            print(f"Total de issues carregadas até agora: {len(issues_list)}")

            # Pega o token da próxima página, se houver
            next_token = data.get("nextPageToken")
            if not next_token or len(issues) == 0:
                break  # Sai se não houver próxima página
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
            'cdb_dba': issue['fields'].get('customfield_13811', None),
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
    df_propostas_ult_dia_util = pd.DataFrame(issues_data)

    # Tratando parecer
    def extrair_parecer(parecer):
        try:
            if not parecer or 'content' not in parecer:
                return None

            texto_final = []

            for bloco in parecer['content']:
                if 'content' in bloco:
                    for parte in bloco['content']:
                        if parte.get('type') == 'text':
                            texto_final.append(parte.get('text', ''))
                    texto_final.append('\n')  # quebra entre blocos (parágrafos)

            return ''.join(texto_final).strip()
        except Exception as e:
            print(f"[Erro ao extrair parecer]: {e}")
        return None

    df_propostas_ult_dia_util['parecer'] = df_propostas_ult_dia_util['parecer'].apply(extrair_parecer)



    # Regra para atualizarmos as issues do df_propostas_ult_dia_util na base histórica

    # Novas issues para atualização no histórico
    issues_novas = df_propostas_ult_dia_util['issue_key'].unique()

    # Excluindo as linhas antigas dessas issues no histórico
    print('Linhas antes da exclusão das issues:')
    print(len(df_historico_propostas))

    df_historico_propostas = df_historico_propostas[~df_historico_propostas['issue_key'].isin(issues_novas)]

    print('Linhas após exclusão das issues:')
    print(len(df_historico_propostas))

    # Inserindo as novas linhas (update das que ja apareciam na base e inserção das novas issues)
    df_final = pd.concat([df_historico_propostas, df_propostas_ult_dia_util], ignore_index=True)

    print('Quantidade de linhas no df final:')
    print(len(df_final))


    # Atribuindo data atual
    now = datetime.datetime.now(tz=timezone(timedelta(hours=-3)))
    df_final['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_final['year'], df_final['month'], df_final['day'] = now.year, now.month, now.day

    df_final = df_final.reset_index(drop=True)


    # Salvando Output
    logger = LoggingMixin().log 

    try:
        logger.info("Iniciando salvamento das informações")

        BUCKET_SOURCE_RAW = "jira"
        FOLDER_DESTINATION_RAW = 'propostas'

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