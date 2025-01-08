# Carregando libs
import pandas as pd
import joblib, logging
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from io import BytesIO
import os
import base64, requests, sys, json


def chamando_serasa(access_params=None,  **kwargs):

    # Conectando com Minio para pegar input da base
    minio_raw = Minio(
        "api-raw.alpe.com.br",
        access_key = 'B7q0avvSIpSdyGPXWnEC',
        secret_key = 'PhMhRQSQ6YJU8fn2qKhDLM017cQPrlCz1YbM8IwU'
    )


    # Connection validation
    try:
        # Try to list the buckets
        buckets = minio_raw.list_buckets()
        
        # If the connection was successful, print the buckests
        print("Conexão bem-sucedida. Lista de buckets disponíveis:")
        for bucket in buckets:
            print(bucket.name)
        
    except Exception as e:
            # If the connection was failed, print the error message
            print(f"Erro ao conectar ao MinIO: {e}")


    # Bucket and Folder_Destination
    BUCKET_SOURCE_RAW = "pre-aprovado-lote"
    FOLDER_DESTINATION_RAW = 'year=2025/month=1/day=6'
    file_name = 'SAIDA_0601_RESPOSTA_CALCULADA.xlsx'
    file_path = f'{FOLDER_DESTINATION_RAW}/{file_name}'


    # Uploading Excel File
    response = minio_raw.get_object(BUCKET_SOURCE_RAW, file_path)
    file_data = BytesIO(response.read())
    base_analisar = pd.read_excel(file_data)

    base_analisar['cnpj_raiz'] = base_analisar['cnpj_raiz'].astype(str).str.zfill(8)

    # Separando os casos que seguem analise
    segue_analise_prefiltro = base_analisar[(base_analisar['resposta'] == 'SEGUE') | (base_analisar['resposta'] == 'MESA')]

    def retrieve(cnpj, token):
        ## Chamada da API do exrep para consulta do Serasa
        logging.info(f'Chamada do Serasa para o CNPJ {cnpj}')
        url = f"{access_params['exrep_url']}api/v1/report-executions"
        print(url)
        headers = {
            'Content-Type': "application/json",
            'Authorization': f"Bearer {token}"
        }
        body = [
            {
                "definition": {
                    "id": "yXL"
                },
                "cacheMaxDays": 60,
                "involved": [
                    {
                        "role": "TARGET",
                        "party": {
                            "identifications": [
                                {
                                    "type": "CNPJ",
                                    "value": cnpj
                                }
                            ]
                        }
                    }
                ],
                "synchronous": "true"
            }
        ]

        try:
            res = requests.post(url, json=body, headers=headers)
            res.raise_for_status()
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

        # Independente do status, mostrar a resposta
        if res is not None:
            logging.info(f"Raw Response for CNPJ {cnpj}: {res.text}")
        else:
            logging.warning(f"No response for CNPJ {cnpj}.")

    # Autenticação no Keycloak
    client_id = access_params['exrep_client_id']
    client_secret = access_params['exrep_client_secret']
    authorization = base64.b64encode(bytes(client_id + ":" + client_secret, "ISO-8859-1")).decode("ascii")

    keycloak = access_params['keycloack_token_url']
    print(keycloak)
    content_headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Authorization": f"Basic {authorization}",
    "Accept": "*/*"
    }
    request_body = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }
    auth = None
    token = None

    try:
        auth = requests.post(keycloak, data=request_body, headers=content_headers)
        auth.raise_for_status()
        result = json.loads(auth.text)
        token = result['access_token']
        logging.info(f"Token recebido: {token}")
    except Exception as e:
        logging.error(f"Erro durante a autenticação: {e}")
        return

    try:
        cnpjs = segue_analise_prefiltro['documento_sem_formatacao'].unique()
        logging.info(f"CNPJs a serem analisados: {cnpjs}")
    except Exception as e:
        logging.error(f"Erro ao processar CNPJs: {e}")
        return

    for cnpj in cnpjs:
        retrieve(cnpj, token)