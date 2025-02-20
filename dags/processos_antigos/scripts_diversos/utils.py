from urllib.parse import urlparse
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from airflow.models import Variable

access_params = {
            "trino_endpoint": Variable.get("TRINO_ENDPOINT"),
            "trino_port": Variable.get("TRINO_PORT"),
            "trino_user": Variable.get("TRINO_USER"),
            "trino_password": Variable.get("TRINO_PASSWORD")
        }

def extract_path_from_url(url):
    parsed_url = urlparse(url)
    extracted_path = parsed_url.path.replace("/raw/browser/", "")
    return extracted_path


def get_trino_connection():
    print("host: ", access_params["trino_endpoint"])
    return connect(
       host= access_params["trino_endpoint"],
       port= access_params["trino_port"],
       auth=BasicAuthentication(access_params["trino_user"], access_params["trino_password"]),
       http_scheme="https"
    )

def execute_query(conn, query):

    cur = conn.cursor()
    cur.execute(query)
    return cur.fetchall()