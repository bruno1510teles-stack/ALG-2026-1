import logging, base64, requests
from jira import JIRA

def selecionar_politica(access_params=None,  **kwargs):

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
