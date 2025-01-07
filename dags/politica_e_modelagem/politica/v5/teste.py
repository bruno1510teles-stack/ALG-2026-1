# Carregando libs
import pandas as pd
import joblib, logging
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from io import BytesIO
import os
import base64, requests, sys, json
import time

def chamando_serasa(access_params=None,  **kwargs):

    def get_token(access_params):
        client_id = access_params['exrep_client_id']
        client_secret = access_params['exrep_client_secret']
        authorization = base64.b64encode(bytes(client_id + ":" + client_secret, "ISO-8859-1")).decode("ascii")

        keycloak = access_params['keycloack_token_url']
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
        try:
            auth = requests.post(keycloak, data=request_body, headers=content_headers)
            auth.raise_for_status()
            result = json.loads(auth.text)
            token = result['access_token']
            expires_in = result.get('expires_in', 300)  # tempo de expiração em segundos (default para 5 minutos)
            expiration_time = time.time() + expires_in - 60  # ajusta para renovar antes da expiração
            logging.info(f"Token recebido: {token}, expira em {expires_in} segundos.")
            return token, expiration_time
        except Exception as e:
            logging.error(f"Erro durante a autenticação: {e}")
            return None, None


        
    # Pegando DF tarefa anterior
    ti = kwargs['ti']
    base_pre_filtro_dict = ti.xcom_pull(task_ids='pre_filtro_task')
    base_pre_filtro = pd.DataFrame(base_pre_filtro_dict)

    # Separando os casos que seguem analise
    segue_analise_prefiltro = base_pre_filtro[(base_pre_filtro['resposta'] == 'SEGUE') | (base_pre_filtro['resposta'] == 'MESA')]

    def retrieve(cnpj, token):
        ## Chamada da API do exrep para consulta do Serasa
        logging.info(f'Chamada do Serasa para o CNPJ {cnpj}')
        url = f"{access_params['exrep_url']}api/v1/report-executions"
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

        if res is not None:
            logging.info(f"Raw Response for CNPJ {cnpj}: {res.text}")
        else:
            logging.warning(f"No response for CNPJ {cnpj}.")

    # Pega o token e o tempo de expiração
    token, expiration_time = get_token(access_params)

    try:
        cnpjs = segue_analise_prefiltro['documento_sem_formatacao'].unique()
        logging.info(f"CNPJs a serem analisados: {cnpjs}")
    except Exception as e:
        logging.error(f"Erro ao processar CNPJs: {e}")
        return

    for cnpj in cnpjs:
        # Verifica se o token está prestes a expirar e renova se necessário
        if time.time() > expiration_time:
            logging.info("Renovando o token de autenticação...")
            token, expiration_time = get_token(access_params)
            if token is None:
                logging.error("Não foi possível renovar o token.")
                return

        retrieve(cnpj, token)