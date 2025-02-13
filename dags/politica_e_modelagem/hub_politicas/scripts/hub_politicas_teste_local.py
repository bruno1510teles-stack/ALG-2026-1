from hub_politicas import aplicar_politica
import os

access_params = {
    "trino_endpoint": os.getenv("TRINO_ENDPOINT"),
    "trino_port": "443",
    "trino_user": os.getenv("TRINO_USER"),
    "trino_password": os.getenv("TRINO_ACCESS_KEY"),
    "jira_url": "https://alpe.atlassian.net",
    "jira_api_token": "o3VowwH342fmWdSAkvstD899",
    "jira_api_user": "jira.service@alpe.com.br",
    "jira_project": "CMGH",
    "jira_max_result": 10
}

if __name__ == "__main__" :
    aplicar_politica(access_params=access_params)