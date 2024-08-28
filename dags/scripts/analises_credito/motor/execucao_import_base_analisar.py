# Importando Libs
import requests
import pandas as pd
import base64
from minio import Minio
from io import BytesIO

def base_analisar(access_params=None):

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

    # Query para buscar os tickets da fila desejada
    query = {
        "jql": "project = cmgt AND Política = 'Política 2' AND status = 'Analyzing Credit Score'",
        "fields": ["key", # ISSUE_JIRA
                "customfield_13729", # CNPJ
                "customfield_13732", # LIMITE ALPE
                "customfield_13808", # INAD ALPE 
                "customfield_13739", # PGID FN
                "customfield_13719" # NOME PGID FN
                ]  # Substitua pelo campo que contém o CNPJ
    }

    # Fazendo a requisição para o Jira
    response = requests.get(jira_url, headers=headers, params=query)

    if response.status_code == 200:
        # Processando a resposta
        tickets = response.json()['issues']

        # Extraindo os valores dos campos
        data = []
        for ticket in tickets:
            issue_jira = ticket['fields'].get('key')  
            cnpj = ticket['fields'].get('customfield_13729')  
            limite_alpe = ticket['fields'].get('customfield_13732')  
            inad_alpe = ticket['fields'].get('customfield_13808')  
            pgid = ticket['fields'].get('customfield_13739')  
            nome_pgid = ticket['fields'].get('customfield_13719')
            
            data.append({
                'issue_jira': issue_jira,
                'CNPJ': cnpj,
                'limite_alpe': limite_alpe,
                'inad_alpe': inad_alpe,
                'pgid' : pgid,
                'nome_pgid': pgid
            })
        
        # Criando um DataFrame com os CNPJs
        df = pd.DataFrame(data)
        print(f"Quantidade de CNPJs na fila política v2: {df.shape[0]}")
        print(f"Quantidade de CNPJs na fila aberto por fornecedor: {df.groupby('nome_pgid')['CNPJ'].size()}")
    else:
        print(f"Failed to fetch data from Jira: {response.status_code}")

    # Passo 1: Remover a máscara de valor e converter para numérico
    if df['limite_alpe'].notna().any():
        df['limite_alpe'] = df['limite_alpe'].replace({'R\$ ': '', '\.': ''}, regex=True)
        df['limite_alpe'] = pd.to_numeric(df['limite_alpe'], errors='coerce')  

    if df['inad_alpe'].notna().any():
        df['inad_alpe'] = df['inad_alpe'].replace({'R\$ ': '', '\.': ''}, regex=True)
        df['inad_alpe'] = pd.to_numeric(df['inad_alpe'], errors='coerce')  # Converte para float, substituindo erros por NaN

    # Passo 3: Criar as colunas de flag com base na lógica fornecida
    df['limite_alpe_flag'] = df['limite_alpe'].apply(lambda x: x > 0 if pd.notna(x) else False)
    df['inad_alpe_flag'] = df['inad_alpe'].apply(lambda x: 'SIM' if pd.notna(x) and x > 0 else 'NAO')

    # Passo 4: Deixar o nome padrão
    # Remover as colunas originais 'limite_alpe' e 'inad_alpe'
    df.drop(columns=['limite_alpe', 'inad_alpe'], inplace=True)

    # Renomear as colunas de flag para os nomes originais
    df.rename(columns={'limite_alpe_flag': 'limite_alpe', 'inad_alpe_flag': 'inad_alpe'}, inplace=True)

    # Exibir o DataFrame resultante
    print(df.head(10))

    # Salvando Output

    BUCKET_SOURCE_REFINED = "motor"
    FOLDER_DESTINATION_REFINED = 'analise_credito/auxiliar'

    # Conectando na refined
    client = Minio(
        access_params['endpoint_url_refined'],
        access_key=access_params['aws_access_key_id_refined'],
        secret_key=access_params['aws_secret_access_key_refined'],
    )


    # Nome do arquivo CSV que você deseja criar
    file_out = f'LANDING_BASE_ANALISAR.csv'

    csv_bytes = df.to_csv(index=False, sep=';').encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)

    client.put_object(f'{BUCKET_SOURCE_REFINED}',
                        f'{FOLDER_DESTINATION_REFINED}/{file_out}',
                            data=csv_buffer,
                            length=len(csv_bytes))