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

    # Calcular a data de ontem
    data_hoje = pd.to_datetime('today').date()

    result_rami = []
    
    # Filtrando as propostas do dia
    propostas_dia = df[df['data_criado'] == data_hoje]

    # Contagem total de propostas no DataFrame
    propostas_total = len(df)

    # Contagem de propostas por 'ramificacao_motor_desc' no DataFrame todo
    ramificacao_count_total = df.groupby('ramificacao_motor_desc').size()

    # Contagem de propostas por 'ramificacao_motor_desc' no dia filtrado
    ramificacao_count_dia = propostas_dia.groupby('ramificacao_motor_desc').size()

    # Garantindo que ambas as contagens (total e do dia) tenham o mesmo índice
    ramificacao_count_total = ramificacao_count_total.reindex(ramificacao_count_dia.index, fill_value=0)

    # Calculando o percentual diário por 'ramificacao_motor_desc' (percentual de propostas do dia sobre o total de propostas do dia)
    total_dia = ramificacao_count_dia.sum()  # Soma total das propostas no dia
    
    # Calculando o percentual diário (percentual de propostas no dia sobre o total diário)
    percent_dia = (ramificacao_count_dia / total_dia) * 100 if total_dia > 0 else 0

    # Calculando o percentual histórico (percentual de propostas no dia sobre o total geral)
    percent_hist = (ramificacao_count_total / propostas_total) * 100 if propostas_total > 0 else 0

    # Calculando a variação entre o percentual diário e o percentual histórico
    variacao_percentual = percent_dia - percent_hist * 1

    # Criando o DataFrame final com as contagens e percentuais
    result_rami = pd.DataFrame({
        'Ramificação': ramificacao_count_dia.index,
        'Propostas Dia': ramificacao_count_dia.values,
        # Percentuais com 2 casas decimais, símbolo de "%" e vírgula como separador decimal
        'Dia %': [f"{round(val, 2):.2f}".replace('.', ',') + " %" if isinstance(val, (int, float)) else "0,00 %" for val in percent_dia.values],
        'Histórico %': [f"{round(val, 2):.2f}".replace('.', ',') + " %" if isinstance(val, (int, float)) else "0,00 %" for val in percent_hist.values],
        'Variação Hoje x Histórico %': [f"{round(val, 2):.2f}".replace('.', ',') + " %" if isinstance(val, (int, float)) else "0,00 %" for val in variacao_percentual]
    })

    # Exibindo o resultado final
    print(result_rami.head())

    # Exportando para Excel
    # result_rami.to_excel(r'C:\Users\kevin.cardoso\Downloads\jira_result.xlsx', index=False, sheet_name='teste')

    # print("Resultados exportados para o Excel com sucesso!")

    # Definindo a data de execução (com base na data do primeiro item do DataFrame, como exemplo)
    data_execucao = data_hoje if not df.empty else 'Data não disponível'
    
    # URL do Webhook do Microsoft Teams
    webhook_url = "https://yandehbr.webhook.office.com/webhookb2/d25ed332-49c9-4cf9-9314-3a18c680afd8@fe284b6f-c6d2-4028-badb-7d0c22aef0ae/IncomingWebhook/5144cf970df44507b307d94ba6b41578/bd7b1c42-afa1-4108-9114-508dccf195b1/V2h8s62OxtgIsUmlvAr6G6yrrxUGkuyCI-4aCDr5t1tZg1"


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
                "activitySubtitle": f"Data de Execução: {data_execucao}",
                "activityText": (
                    f"Prezados(as), boa tarde,<br><br>"
                    f"Em relação ao resumo de propostas por Ramificação, registramos:<br><br>"
                    f"<table style='width:100%; border: 1px solid black; border-collapse: collapse;'>"
                    f"<tr><th style='border: 1px solid black; padding: 5px;'>Ramificação</th>"
                    f"<th style='border: 1px solid black; padding: 5px;'>Propostas Dia</th>"
                    f"<th style='border: 1px solid black; padding: 5px;'>Percentual Dia %</th>"
                    f"<th style='border: 1px solid black; padding: 5px;'>Percentual Histórico %</th>"
                    f"<th style='border: 1px solid black; padding: 5px;'>Variação Hoje x Histórico %</th></tr>"
                    + "".join([  # Itera sobre os dados de 'result_rami' para gerar a tabela
                        f"<tr><td style='border: 1px solid black; padding: 5px;'>{row['Ramificação']}</td>"
                        f"<td style='border: 1px solid black; padding: 5px;'>{row['Propostas Dia']}</td>"
                        f"<td style='border: 1px solid black; padding: 5px;'>{row['Dia %']}</td>"
                        f"<td style='border: 1px solid black; padding: 5px;'>{row['Histórico %']}</td>"
                        f"<td style='border: 1px solid black; padding: 5px;'>{row['Variação Hoje x Histórico %']}</td></tr>"
                        for index, row in result_rami.iterrows()  # Itera sobre o DataFrame result_rami
                    ])
                    # Adicionando a linha de totais
                    + f"<tr><td style='border: 1px solid black; padding: 5px; font-weight: bold;'>Totais</td>"
                    + f"<td style='border: 1px solid black; padding: 5px; font-weight: bold;'>{result_rami['Propostas Dia'].sum()}</td>"
                    + f"<td style='border: 1px solid black; padding: 5px; font-weight: bold;'>{f'{percent_dia.sum():.2f}'.replace('.', ',')} %</td>"
                    + f"<td style='border: 1px solid black; padding: 5px; font-weight: bold;'>{f'{percent_hist.sum():.2f}'.replace('.', ',')} %</td>"
                    + f"<td style='border: 1px solid black; padding: 5px; font-weight: bold;'>{f'{variacao_percentual.sum():.2f}'.replace('.', ',')} %</td></tr>"
                    + "</table><br><br>"
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