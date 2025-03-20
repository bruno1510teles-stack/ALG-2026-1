# Carregando libs
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import os, pytz
from datetime import datetime
import time
import base64
import requests
from airflow.models import Variable


def envia_alerta_teams (access_params=None,  **kwargs):

    # Pegando DF tarefa anterior
    # Recupera o objeto ti (task instance) via kwargs
    ti = kwargs['ti']
    base_analisar_dict  = ti.xcom_pull(task_ids='politica_task')
    base_analisar = pd.DataFrame(base_analisar_dict)

    base_enviar_alerta = base_analisar.loc[base_analisar['resolucao'] != "MESA"]

    def notificar_falha_teams(base_analisar):
        url = "https://yandehbr.webhook.office.com/webhookb2/aff1add1-1e5e-445d-9644-f7d9ab677641@fe284b6f-c6d2-4028-badb-7d0c22aef0ae/IncomingWebhook/cd64a656b86b4db6a8a64a74153a8555/e3ad1a1a-7716-40ee-ab81-0f05650df5dc/V2M-crEG-kOlO8wQffCBWAHSBeR29YtNktVPx1gvoiR4M1"
        
        # Verifica se há dados na base 'base_analisar'
        if not base_analisar.empty:
            # Pega a primeira linha da base (ajuste conforme necessário para iterar sobre outras linhas)
            issue = base_analisar['issue_jira'].iloc[0]
            valor_aprovado = base_analisar['valor_aprovado'].iloc[0]
            resolucao = base_analisar['resolucao'].iloc[0]

            # Criando a mensagem personalizada
            mensagem = {
                "title": f"Politica Desafiante",
                "text": f"""
                Issue: {issue} processada com sucesso!
                Valor Aprovado: {valor_aprovado}
                Resolução: {resolucao}
                """
            }
        else:
            return
        
        # Enviando a mensagem para o Teams via webhook
        response = requests.post(url, json=mensagem)
        
        # Checando se a requisição foi bem-sucedida
        if response.status_code != 200:
            print(f"Falha ao enviar a notificação para o Teams: {response.status_code} - {response.text}")
        else:
            print("Notificação enviada com sucesso!")

    # Chamada para a função de notificação com base na filtragem
    notificar_falha_teams(base_enviar_alerta)

