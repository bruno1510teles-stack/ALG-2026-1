# Importando Bibliotecas
import requests
import pandas as pd
import numpy as np
import math
import base64
from io import BytesIO
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from minio.error import S3Error
from requests.auth import HTTPBasicAuth
import json
from deltalake import write_deltalake, DeltaTable
from datetime import datetime, timezone, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText




def enviar_notif(access_params=None, **kwargs):
    # Conectando ao Trino para Leitura
    conn = connect(
        host='trino.alpe.com.br',
        port=443,
        user='trinodados',
        auth=BasicAuthentication('trinodados', 'hosgzPvuhyXkP<j}RyT+'),
        http_scheme="https",
    )

    # Função para executar consultas
    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        return pd.DataFrame(rows, columns=columns)
    
    query_jira_refined = """
        SELECT *
        FROM deltalakerefined.jira.propostas
    """
    
    # Executa a consulta e carrega o DataFrame
    df = execute_query(conn, query_jira_refined)
    
    # Exibindo o número de colunas carregadas e disponíveis no DataFrame
    print("Colunas carregadas e disponíveis no DataFrame:")
    print(len(df.columns))  # Isso exibe o número de colunas

    # Converter a coluna 'data_criado' para datetime e extrair apenas a data
    df['data_criado'] = pd.to_datetime(df['data_criado'], errors='coerce').dt.date

    # Filtrando apenas as linhas onde 'tipo_analista' é igual a "Motor"
    df_motor = df[df['tipo_analista'] == "Motor"]

    # Calcular a data de hoje
    data_hoje = pd.to_datetime('today').date()

    result_rami = []

    # Filtrando as propostas do dia no DataFrame filtrado (apenas com 'Motor')
    propostas_dia = df_motor[df_motor['data_criado'] == data_hoje]

    # Contagem total de propostas no DataFrame filtrado
    propostas_total = len(df_motor)

    # ----- Contagens e percentuais por política e ramificação -----
    group_total = df_motor.groupby(['politica_desc', 'ramificacao_motor_desc', 'parecer_desc']).size()
    group_dia = propostas_dia.groupby(['politica_desc', 'ramificacao_motor_desc', 'parecer_desc']).size()

    # Garantindo alinhamento entre total e dia
    group_total = group_total.reindex(group_dia.index, fill_value=0)

    # Soma total das propostas no dia
    total_dia = group_dia.sum()
    percent_dia = (group_dia / total_dia * 100) if total_dia > 0 else group_dia * 0
    percent_hist = (group_total / propostas_total * 100) if propostas_total > 0 else group_total * 0
    variacao_percentual = round((percent_dia / percent_hist - 1) * 100, 2)

    # Criando o DataFrame final com as contagens e percentuais
    result_rami = pd.DataFrame({
        'Política': [index[0] for index in group_dia.index],
        'Ramificação': [index[1] for index in group_dia.index],
        'Parecer': [index[2] for index in group_dia.index],
        'Propostas Dia': group_dia.values,
        'Dia %': [f"{round(val, 2):.2f}".replace('.', ',') + " %" for val in percent_dia],
        'Histórico %': [f"{round(val, 2):.2f}".replace('.', ',') + " %" for val in percent_hist],
        'Variação Hoje x Histórico %': [f"{round(val, 2):.2f}".replace('.', ',') + " %" for val in variacao_percentual]
    })

    # Exibindo o resultado final
    print(result_rami)


    # Exportando para Excel
    # result_rami.to_excel(r'C:\Users\kevin.cardoso\Downloads\jira_result.xlsx', index=False, sheet_name='teste')

    # print("Resultados exportados para o Excel com sucesso!")

    # Definindo a data de execução (com base na data do primeiro item do DataFrame, como exemplo)
    data_execucao = data_hoje if not df.empty else 'Data não disponível'
    
    # URL do Webhook do Microsoft Teams
    webhook_url = "https://yandehbr.webhook.office.com/webhookb2/11355f7b-95b1-414a-8ace-fd3555a3f761@fe284b6f-c6d2-4028-badb-7d0c22aef0ae/IncomingWebhook/339eb623c8ea44608834e99bb2381457/bd7b1c42-afa1-4108-9114-508dccf195b1/V25zJILk1PdfrDJhZO9ChlnubijOHK8YHVVMSo6p5udE01"


    # Criando a mensagem para o Teams
    mensagem = {
    "@type": "MessageCard",
    "@context": "http://schema.org/extensions",
    "summary": "Resumo Diário de Propostas",
    "themeColor": "0078D4",
    "title": "Resumo Diário de Propostas",
    "sections": [
        {
            "activityTitle": "📊 Resumo Diário de Propostas",
            "activitySubtitle": f"Data de Execução: {data_hoje}",
            "activityText": (
                f"Prezados(as), boa tarde,<br><br>"
                f"Segue o resumo de propostas por Política, Ramificação e Parecer:<br><br>"
                f"<table style='width:100%; border: 1px solid black; border-collapse: collapse;'>"
                f"<tr><th style='border: 1px solid black; padding: 5px;'>Política</th>"
                f"<th style='border: 1px solid black; padding: 5px;'>Ramificação</th>"
                f"<th style='border: 1px solid black; padding: 5px;'>Parecer</th>"
                f"<th style='border: 1px solid black; padding: 5px;'>Propostas Dia</th>"
                f"<th style='border: 1px solid black; padding: 5px;'>Percentual Dia %</th>"
                f"<th style='border: 1px solid black; padding: 5px;'>Percentual Histórico %</th>"
                f"<th style='border: 1px solid black; padding: 5px;'>Variação Hoje x Histórico %</th></tr>"
                + "".join([  # Itera sobre os dados de 'result_rami' para gerar a tabela
                    f"<tr>"
                    f"<td style='border: 1px solid black; padding: 5px;'>{row['Política']}</td>"
                    f"<td style='border: 1px solid black; padding: 5px;'>{row['Ramificação']}</td>"
                    f"<td style='border: 1px solid black; padding: 5px;'>{row['Parecer']}</td>"
                    f"<td style='border: 1px solid black; padding: 5px;'>{row['Propostas Dia']}</td>"
                    f"<td style='border: 1px solid black; padding: 5px;'>{row['Dia %']}</td>"
                    f"<td style='border: 1px solid black; padding: 5px;'>{row['Histórico %']}</td>"
                    f"<td style='border: 1px solid black; padding: 5px;'>{row['Variação Hoje x Histórico %']}</td>"
                    f"</tr>"
                    for _, row in result_rami.iterrows()
                ])
                + f"</table><br><br>"
                f"Atenciosamente,<br>"
                f"Equipe de Políticas de Modelagem de Crédito"
            )
        }
    ]
}




    # Enviar a notificação para o Teams
    try:
        response = requests.post(webhook_url, json=mensagem)
        response.raise_for_status()  # Verifica se houve falha na requisição
        print("Notificação enviada com sucesso para o Microsoft Teams!")
    except requests.exceptions.RequestException as e:
        print(f"Erro ao enviar notificação para o Microsoft Teams: {e}")