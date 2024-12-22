from pre_filtro_comun import analise_pre_filtro
import os

access_params = {
    "trino_endpoint": os.getenv("TRINO_ENDPOINT"),
    "trino_port": "443",
    "trino_user": os.getenv("TRINO_USER"),
    "trino_password": os.getenv("TRINO_ACCESS_KEY"),
    "jira_url": "https://alpe.atlassian.net",
    "jira_api_token": "o3VowwH342fmWdSAkvstD899",
    "jira_api_user": "jira.service@alpe.com.br",
    "jira_project": "CMGH"
}

if __name__ == "__main__" :
    analise_pre_filtro(access_params=access_params)