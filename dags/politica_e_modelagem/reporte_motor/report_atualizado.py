# Importando bibliotecas
import requests
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import pandas as pd
import numpy as np
import base64
from minio import Minio
from io import BytesIO
from requests.auth import HTTPBasicAuth
from deltalake import write_deltalake, DeltaTable
from datetime import datetime, timezone, timedelta
from tabulate import tabulate


def report_motor_atualizado(access_params=None,  **kwargs):

    # Define a data de execução para a mensagem
    hoje = datetime.now().date()
    if hoje.weekday() == 0:  # 0 = segunda-feira
        data_execucao = hoje - timedelta(days=3)
    else:
        data_execucao = hoje - timedelta(days=1)

    # Conectando ao Trino para Leitura
    conn = connect(
        host=access_params['trino_endpoint'],
        port=access_params['trino_port'],
        user=access_params['trino_user'],
        auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
        http_scheme="https",
    )

    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        return pd.DataFrame(rows, columns=columns)
    

    # Query Reporte
    query_reporte = f"""
            WITH contagens AS (
                SELECT
                    CAST(try_cast(data_resolvido AS date) AS varchar) AS "Data",
                    Politica AS "Política",
                    count(*) AS "Qtde",
                    count(CASE WHEN decisao = 'APROVADO' THEN 1 END) AS "Qtde Aprovada"
                FROM deltalaketrusted.jira.propostas
                WHERE decisor = 'MOTOR'
                AND try_cast(data_resolvido AS date) = 
                    CASE 
                        WHEN EXTRACT(DOW FROM current_date) = 1 
                        THEN current_date - INTERVAL '3' DAY 
                        ELSE current_date - INTERVAL '1' DAY 
                    END
                GROUP BY CAST(try_cast(data_resolvido AS date) AS varchar), politica
            ),
            
            resultados AS (
                SELECT
                    "Data",
                    "Política",
                    "Qtde",
                    "Qtde Aprovada",
                    TRUNCATE(CAST(100.0 * "Qtde Aprovada" AS DECIMAL(10, 4)) / NULLIF("Qtde", 0), 2) AS "Percentual Aprovação"
                FROM contagens
                
                UNION ALL
                
                SELECT
                    'Total Geral' AS "Data",
                    '-' AS "Política",
                    SUM("Qtde") AS "Qtde",
                    SUM("Qtde Aprovada") AS "Qtde Aprovada",
                    TRUNCATE(CAST(100.0 * SUM("Qtde Aprovada") AS DECIMAL(10, 4)) / NULLIF(SUM("Qtde"), 0), 2) AS "Percentual Aprovação"
                FROM contagens
            )
            
            SELECT
                "Data",
                "Política",
                "Qtde",
                "Qtde Aprovada",
                FORMAT('%.2f', "Percentual Aprovação") || '%' AS "(%) Aprovação"
            FROM resultados
            ORDER BY
                "Data"
    """
    report = execute_query(conn, query_reporte)
    print("Relatório obtido com sucesso!")

    # Gera a mensagem Markdown
    if report.empty:
        markdown = f"⚠️ Nenhum resultado encontrado para {data_execucao}"
    else:
        # Converte DF em tabela formatada
        tabela_formatada = tabulate(
            report.values.tolist(),
            headers=report.columns.tolist(),
            tablefmt="pretty"
        )

        # Espaço invisível (para espaçamento no Teams/Markdown)
        invisible_space = "\u200B"
        
        # Extrai o valor do total geral de aprovação da linha 'Total Geral'
        # Filtra o DataFrame e pega o valor da coluna "(%) Aprovação"
        total_aprovacao = report[report['Política'] == '-']['(%) Aprovação'].iloc[0]

        # Monta o markdown final
        markdown = (
            "📊 Relatório Resumido de Decisões do Motor de Crédito (último dia útil)\n\n"
            f"{invisible_space}\n"
            f"📅 Data Referência: {data_execucao}\n\n"
            f"✅ Total Geral de Aprovação: {total_aprovacao}\n"
            "```\n" + tabela_formatada + "\n```\n"
            f"{invisible_space}\n"
        )

    print(markdown)

    # Função para enviar a mensagem formatada ao webhook do Teams
    def enviar_para_webhook(mensagem):
        webhook_url = "https://yandehbr.webhook.office.com/webhookb2/aff1add1-1e5e-445d-9644-f7d9ab677641@fe284b6f-c6d2-4028-badb-7d0c22aef0ae/IncomingWebhook/cd64a656b86b4db6a8a64a74153a8555/e3ad1a1a-7716-40ee-ab81-0f05650df5dc/V2M-crEG-kOlO8wQffCBWAHSBeR29YtNktVPx1gvoiR4M1"
        
        headers = {
            'Content-Type': 'application/json'
        }
        
        payload = {
            "text": mensagem
        }
        
        response = requests.post(webhook_url, json=payload, headers=headers)
        
        if response.status_code == 200:
            print("Mensagem enviada com sucesso para o Teams!")
        else:
            print(f"Falha ao enviar a mensagem. Código de status: {response.status_code}")

    
    # Enviar a mensagem combinada para o webhook
    enviar_para_webhook(markdown)
    
