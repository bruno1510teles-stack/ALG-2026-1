# Importando Bibliotecas
import requests
import pandas as pd
import numpy as np
import math
import base64
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from minio.error import S3Error
from io import BytesIO
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
    
    # Inspecionando colunas do DataFrame
    print("Colunas carregadas e disponíveis no DataFrame:")
    print(df.columns.tolist())

    # Converter a coluna 'data_criado' para date
    df['data_criado'] = pd.to_datetime(df['data_criado'], errors='coerce').dt.date
    # Obtendo a data de hoje
    #data_hoje = pd.to_datetime('today').date()
    
    data_hoje = pd.to_datetime('today').date() - pd.Timedelta(days=1)

    # Filtro de data específico (2024-11-14)
    df_filtrado = df[df['data_criado'] == data_hoje]

    # Verificar se o df_filtrado não está vazio antes de calcular as métricas
    if not df_filtrado.empty:
        # -------- Resultados agrupados por 'politica_desc' --------
        resultado_politica = []  # Inicializando a lista para armazenar os resultados
        for politica, grupo_politica in df_filtrado.groupby('politica_desc'):
            propostas = grupo_politica['issue_key'].count()
            aprovados = (grupo_politica['status_decisao'] == 'Aprovado').sum()
            reprovados = (grupo_politica['status_decisao'] == 'Reprovado').sum()
            nao_atribuido = (grupo_politica['status_decisao'] == 'Decisão não atribuida').sum()

            aprovados_percent = 100.0 * aprovados / propostas if propostas != 0 else 0
            reprovados_percent = 100.0 * reprovados / propostas if propostas != 0 else 0
            nao_atribuido_percent = 100.0 * nao_atribuido / propostas if propostas != 0 else 0

            valor_aprovado = grupo_politica['limite_aprovado'].sum()  # Corrigido para 'limite_aprovado'

            # Média das decisões calculadas com base em todas as datas
            df_all_dates = df  # Considerando todo o DataFrame carregado sem o filtro de data
            media_aprovado_diario = (df_all_dates['status_decisao'] == 'Aprovado').sum() / df_all_dates['data_criado'].nunique()
            media_reprovado_diario = (df_all_dates['status_decisao'] == 'Reprovado').sum() / df_all_dates['data_criado'].nunique()
            media_nao_atribuido_diario = (df_all_dates['status_decisao'] == 'Decisão não atribuida').sum() / df_all_dates['data_criado'].nunique()

            # Adicionando os resultados de cada grupo na lista de resultados
            resultado_politica.append({
                "Política": politica,  # politica_desc
                "Data Referência": '2024-11-14',
                "Propostas": propostas,
                "Aprovados": aprovados,
                "Reprovados": reprovados,
                "Não Atribuído": nao_atribuido,
                "Aprovado (%)": round(aprovados_percent, 2),
                "Reprovado (%)": round(reprovados_percent, 2),
                "Não Atribuído (%)": round(nao_atribuido_percent, 2),
                "Valor Aprovado": valor_aprovado,
                "Média Aprovado Diário": round(media_aprovado_diario),
                "Média Reprovado Diário": round(media_reprovado_diario),
                "Média Não Atribuído Diário": round(media_nao_atribuido_diario)
            })

        # Criando o DataFrame de resultados agrupados por 'politica_desc'
        politica_df = pd.DataFrame(resultado_politica)
        print("Resultados agrupados por Política:")
        print(politica_df)

        # -------- Resultados agrupados por 'ramificacao_motor_desc' --------
        resultado_ramifica = []  # Inicializando a lista para armazenar os resultados
        for ramificacao, grupo_ramificacao in df_filtrado.groupby('ramificacao_motor_desc'):
            propostas = grupo_ramificacao['issue_key'].count()
            aprovados = (grupo_ramificacao['status_decisao'] == 'Aprovado').sum()
            reprovados = (grupo_ramificacao['status_decisao'] == 'Reprovado').sum()
            nao_atribuido = (grupo_ramificacao['status_decisao'] == 'Decisão não atribuida').sum()

            aprovados_percent = 100.0 * aprovados / propostas if propostas != 0 else 0
            reprovados_percent = 100.0 * reprovados / propostas if propostas != 0 else 0
            nao_atribuido_percent = 100.0 * nao_atribuido / propostas if propostas != 0 else 0

            valor_aprovado = grupo_ramificacao['limite_aprovado'].sum()  # Corrigido para 'limite_aprovado'

            # Média das decisões calculadas com base em todas as datas
            df_all_dates = df  # Considerando todo o DataFrame carregado sem o filtro de data
            media_aprovado_diario = (df_all_dates['status_decisao'] == 'Aprovado').sum() / df_all_dates['data_criado'].nunique()
            media_reprovado_diario = (df_all_dates['status_decisao'] == 'Reprovado').sum() / df_all_dates['data_criado'].nunique()
            media_nao_atribuido_diario = (df_all_dates['status_decisao'] == 'Decisão não atribuida').sum() / df_all_dates['data_criado'].nunique()

            # Adicionando os resultados de cada grupo na lista de resultados
            resultado_ramifica.append({
                "Ramificação Motor": ramificacao,  # ramificacao_motor_desc
                "Data Referência": '2024-11-14',
                "Propostas": propostas,
                "Aprovados": aprovados,
                "Reprovados": reprovados,
                "Não Atribuído": nao_atribuido,
                "Aprovado (%)": round(aprovados_percent, 2),
                "Reprovado (%)": round(reprovados_percent, 2),
                "Não Atribuído (%)": round(nao_atribuido_percent, 2),
                "Valor Aprovado": valor_aprovado,
                "Média Aprovado Diário": round(media_aprovado_diario),
                "Média Reprovado Diário": round(media_reprovado_diario),
                "Média Não Atribuído Diário": round(media_nao_atribuido_diario)
            })

        # Criando o DataFrame de resultados agrupados por 'ramificacao_motor_desc'
        ramificacao_df = pd.DataFrame(resultado_ramifica)
        print("Resultados agrupados por Ramificação Motor:")
        print(ramificacao_df)

    else:
        print("Nenhum dado encontrado para a data especificada.")

    # Função para calcular a média diária consolidada por grupo (sem filtrar por data_criado)
    def calc_media_diaria_consolidada(df_completo, segmento_col, segmento_valor, status):
        # Filtra o DataFrame conforme o segmento
        segmento_df = df_completo[df_completo[segmento_col] == segmento_valor]
        
        # Conta a quantidade de status por dia
        status_por_dia = segmento_df[segmento_df['status_decisao'] == status].groupby('data_criado').size()
        
        # Se não estiver vazio, calcula a média diária do total de propostas no grupo
        return int(status_por_dia.sum() / len(status_por_dia)) if len(status_por_dia) > 0 else 0

    # Calculando totais gerais
    total_propostas = df_filtrado['issue_key'].count()
    total_aprovadas = (df_filtrado['status_decisao'] == 'Aprovado').sum()
    total_reprovadas = (df_filtrado['status_decisao'] == 'Reprovado').sum()
    nao_atribuido = (df_filtrado['status_decisao'] == 'Decisão não atribuida').sum()

    # Calculando as porcentagens
    aprovados_percent = (total_aprovadas / total_propostas) * 100 if total_propostas > 0 else 0
    reprovados_percent = (total_reprovadas / total_propostas) * 100 if total_propostas > 0 else 0
    nao_atribuido_percent = (nao_atribuido / total_propostas) * 100 if total_propostas > 0 else 0


    # URL do Webhook do Microsoft Teams
    webhook_url = "https://yandehbr.webhook.office.com/webhookb2/0c94e931-a331-49d4-a0cf-bc72233bc19b@fe284b6f-c6d2-4028-badb-7d0c22aef0ae/IncomingWebhook/7b341a107c8c4df88e3a62aabb6743ec/bd7b1c42-afa1-4108-9114-508dccf195b1/V2SZ5OpXLKgfyduQvS07_t9vh0nK1x-qNjiZrRNRARLL81"

    # Mensagem a ser enviada para o Teams
    mensagem = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": "Resumo Diário de Propostas",
        "themeColor": "0078D4",
        "title": "Resumo Diário de Propostas",
        "sections": [
            {
                "activityTitle": "📊 Resumo Diário de Propostas",
                "activitySubtitle": f"Data de Execução: {df_filtrado['data_criado'].iloc[0] if not df_filtrado.empty else 'Data não disponível'}",
                "activityText": (
                    f"Prezados(as), boa tarde,<br><br>"
                    f"Em relação ao resumo de propostas por Política e Ramificação, registramos:<br><br>"
                    f"- <strong>Total de Propostas:</strong> {total_propostas}<br>"
                    f"- <strong>Aprovadas:</strong> {total_aprovadas} ({round(aprovados_percent, 2)}%)<br>"
                    f"- <strong>Reprovadas:</strong> {total_reprovadas} ({round(reprovados_percent, 2)}%)<br>"
                    f"- <strong>Não Atribuído:</strong> {nao_atribuido} ({round(nao_atribuido_percent, 2)}%)<br><br>"
                    f"<strong>Resumo por Política:</strong><br>" +
                    "<br>".join([  # Itera sobre as políticas
                        f"- <strong>{politica}</strong>: {grupo['issue_key'].count()} propostas "
                        f"({(grupo['status_decisao'] == 'Aprovado').sum()} aprovadas, "
                        f"{(grupo['status_decisao'] == 'Reprovado').sum()} reprovadas, "
                        f"{(grupo['status_decisao'] == 'Decisão não atribuida').sum()} não atribuídos)<br>"
                        # Calculando as médias diárias consolidadas por grupo (política)
                        f"  - <strong>Valor Aprovado:</strong> R$ {grupo['limite_aprovado'].sum():,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') + "<br>"
                        for politica, grupo in df_filtrado.groupby('politica_desc')
                    ]) + "<br><br>" +
                    f"<strong>Resumo por Ramificação:</strong><br>" +
                    "<br>".join([  # Itera sobre as ramificações
                        f"- <strong>{ramificacao}</strong>: {grupo['issue_key'].count()} propostas "
                        f"({(grupo['status_decisao'] == 'Aprovado').sum()} aprovadas, "
                        f"{(grupo['status_decisao'] == 'Reprovado').sum()} reprovadas, "
                        f"{(grupo['status_decisao'] == 'Decisão não atribuida').sum()} não atribuídos)<br>"
                        # Calculando as médias diárias consolidadas por grupo (ramificação)
                        f"  - <strong>Valor Aprovado:</strong> R$ {grupo['limite_aprovado'].sum():,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') + "<br>"
                        for ramificacao, grupo in df_filtrado.groupby('ramificacao_motor_desc')
                    ]) + "<br><br>"
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