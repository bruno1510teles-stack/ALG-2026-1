from requests.auth import HTTPBasicAuth
from airflow.models import Variable

def get_acess_token():
    username = Variable.get("TRINO_USER")
    password = Variable.get("TRINO_ACCESS_KEY") 

    return HTTPBasicAuth(username, password)