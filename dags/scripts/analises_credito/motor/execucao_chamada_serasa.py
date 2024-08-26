# Carregando libs
import pandas as pd
import joblib, logging
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from io import BytesIO
import os
import base64, requests, sys, json



def chamando_serasa(access_params=None):

    # VARIAVEIS DE DOS ARQUIVOS
    BUCKET_SOURCE_REFINED = "motor"
    FOLDER_SOURCE_REFINED = 'analise_credito/auxiliar'
    FOLDER_DESTINATION_REFINED = 'analise_credito/auxiliar'

    # Conectando na refined
    client = Minio(
        access_params['endpoint_url_refined'],
        access_key=access_params['aws_access_key_id_refined'],
        secret_key=access_params['aws_secret_access_key_refined'],
    )

    dtype = {
        'cnpj_raiz': str,
        'documento_sem_formatacao': str,
        'razao_social': str,
        'cod_cnae': str,
        'cod_natureza_juridica': str
    }

    # BAIXANDO ARQUIVO A SER ANALISADO
    try:
        file = client.get_object(bucket_name=BUCKET_SOURCE_REFINED, object_name=f'{FOLDER_SOURCE_REFINED}/LANDING_PRE_FILTRO.csv')
        base_pre_filtro = pd.read_csv(BytesIO(file.data), dtype=dtype, sep=';')
    except Exception as e:
        logging.error(f"Erro ao baixar ou ler o arquivo: {e}")
        return

    # Separando os casos que seguem analise
    segue_analise_prefiltro = base_pre_filtro[base_pre_filtro['resposta'] == 'SEGUE']

    def retrieve(cnpj, token):
        ## Chamada da API do exrep para consulta do Serasa
        logging.info(f'Chamada do Serasa para o CNPJ {cnpj}')
        url = f"{access_params['exrep_url']}/api/v1/report-executions"
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
    content_headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {authorization}",
        "User-Agent": "PostmanRuntime/7.37.3",
        "Accept": "*/*",
        "Cache-Control": "no-cache",
        "Postman-Token": "3a1ab209-fe96-4b1e-99e4-45e996eb4211",
        "Host": access_params['exrep_url'],
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Content-Length": "29"
    }
    request_body = {"grant_type": "client_credentials"}
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