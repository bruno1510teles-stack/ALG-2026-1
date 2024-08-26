from airflow.models import Variable
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient

def get_access_token():
    # Acessar as variáveis de ambiente
    client_id = Variable.get('SERASA_CLIENT_ID')
    client_secret = Variable.get('SERASA_CLIENT_SECRET')
    token_url = Variable.get('KEYCLOAK_TOKEN_URL')

    if not client_id or not client_secret or not token_url:
        raise ValueError("As variáveis de ambiente SERASA_CLIENT_ID, SERASA_CLIENT_SECRET e KEYCLOAK_TOKEN_URL devem estar definidas")

    # Configura o cliente OAuth2 com as credenciais
    client = BackendApplicationClient(client_id=client_id)
    oauth = OAuth2Session(client=client)

    # Faz a requisição para obter o token de acesso
    token = oauth.fetch_token(token_url=token_url, client_id=client_id, client_secret=client_secret)

    return token['access_token']