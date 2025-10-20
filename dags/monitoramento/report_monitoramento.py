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


def report_monitoramento (access_params=None):



    data_execucao = datetime.today().date()
    

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

    # Definindo a consulta
    query_tabelas = f"""
    select mt.camada,
        mt.schema,
        mt.tabela,
        mt.ultima_atualizacao,
        date_diff('day', DATE(parse_datetime(mt.ultima_atualizacao, 'yyyy-MM-dd HH:mm:ss')), current_date) AS dias_defasados
    from deltalakerefined.monitoramento.monitoramento_tabelas mt
    where mt.schema not in (
        'yandeh', 
        'antifraude', 
        'receita_federal',
        'pessoas_e_organizacoes',
        'receita_federal_historico',
        'risco')
        and DATE(parse_datetime(mt.ultima_atualizacao, 'yyyy-MM-dd HH:mm:ss')) <> CURRENT_DATE
        and DATE(parse_datetime(mt.atualizado_em, 'yyyy-MM-dd HH:mm:ss')) = DATE '{data_execucao}'
        and mt.tabela not in ('fat_pag_join',
        'faturamento_externo_arcelor',
        'faturamento_externo_arcelor_aux',
        'retorno_consolidado',
        'recompra_consolidado',
        'aquisicao_consolidado',
        'aquisicao',
        'pontualidade',
        'pagamento_externo_arcelor',
        'propostas_boletos_aux_vop',
        'pre_filtro')
    order by ultima_atualizacao DESC

        """
    df_tabelas = execute_query(conn, query_tabelas)

    print(f"Quantidade de tabelas não atualizadas:{df_tabelas.shape[0]}")

    if df_tabelas.empty:
        markdown = f"⚠️ Nenhuma tabela desatualizada foi encontrada no reporte monitoramento em {data_execucao}"
    else:
        # Converte DF em tabela formatada
        tabela_formatada = tabulate(
            df_tabelas.values.tolist(),
            headers=df_tabelas.columns.tolist(),
            tablefmt="pretty"
        )

        # Espaço invisível (para espaçamento no Teams/Markdown)
        invisible_space = "\u200B"

        # Monta o markdown final
        markdown = (
            "📊 Resumo Diário de Monitoramento de Tabelas\n\n"
            f"{invisible_space}\n"
            f"📅 Data Referência: {data_execucao}\n\n"
            f"🧾 Total de tabelas retornadas: {len(df_tabelas)}\n\n"
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