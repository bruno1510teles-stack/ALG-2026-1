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

def report_motor_desafiante_hora_hora (access_params=None):

    data_execucao = datetime.today().strftime('%Y-%m-%d')

    # Conectando ao Trino para Leitura
    conn = connect(
        host='trino.alpe.com.br',
        port=443,
        user='trinodados',
        auth=BasicAuthentication('trinodados', 'hosgzPvuhyXkP<j}RyT+'),
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
    query_acomp_desafiante = f"""
                            select 	sub.faixa_valor_solicitado as "Faixa Valor Solicitado",
                                count(sub.issue_key) as "Entrantes",
                                sum(case when sub.categoria_decisor = 'MESA' then 1 else 0 end) as "Derivadas Mesa",
                                sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'REPROVADO' then 1 else 0 end) as "Reprovadas Motor",
                                sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then 1 else 0 end) as "Aprovadas Motor",
                                sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.limite_aprovado else 0 end) as "Valor Aprovado Motor"
                            from (
                                select 	a.*,
                                        CASE
                                            WHEN  limite_pedido <= 50000 THEN '01 - Até 50K'
                                            WHEN  limite_pedido > 50000 AND  limite_pedido <= 60000 THEN '02 - 50K - 60K'
                                            WHEN  limite_pedido > 60000 AND  limite_pedido <= 70000 THEN '03 - 60K - 70K'
                                            WHEN  limite_pedido > 70000 AND  limite_pedido <= 80000 THEN '04 - 70K - 80K'
                                            WHEN  limite_pedido > 80000 AND  limite_pedido <= 90000 THEN '05 - 80K - 90K'
                                            WHEN  limite_pedido > 90000 AND  limite_pedido <= 100000 THEN '06 - 90K - 100K'
                                            WHEN  limite_pedido > 100000 AND  limite_pedido <= 110000 THEN '07 - 100K - 110K'
                                            WHEN  limite_pedido > 110000 AND  limite_pedido <= 120000 THEN '08 - 110K - 120K'
                                            WHEN  limite_pedido > 120000 AND  limite_pedido <= 130000 THEN '09 - 120K - 130K'
                                            WHEN  limite_pedido > 130000 AND  limite_pedido <= 140000 THEN '10 - 130K - 140K'
                                            WHEN  limite_pedido > 140000 AND  limite_pedido <= 150000 THEN '11 - 140K - 150K'
                                            ELSE '12 - >150K'
                                        END AS faixa_valor_solicitado
                                from deltalaketrusted.jira.propostas as a
                                where politica = 'DESAFIANTE'
                                and date(data_criado) = date(timestamp '{data_execucao}')
                            ) as sub
                            group by sub.faixa_valor_solicitado
                    """


    query_acomp_desafiante_consolidado = f"""
                            select 
                            sub.faixa_valor_solicitado as "Faixa Valor Solicitado",
                            count(sub.issue_key) as "Entrantes",
                            sum(case when sub.categoria_decisor = 'MESA' then 1 else 0 end) as "Derivadas Mesa",
                            sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'REPROVADO' then 1 else 0 end) as "Reprovadas Motor",
                            sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then 1 else 0 end) as "Aprovadas Motor",
                            sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.limite_aprovado else 0 end) as "Valor Aprovado Motor",
                            sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.vop else 0 end) as "VOP", 
                            sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.vencido else 0 end) as "Vencido",
                            sum(case when sub.categoria_decisor = 'MOTOR' and sub.decisao = 'APROVADO' then sub.vop_over_30 else 0 end) as "Over 30"
                            
                        from (
                        select 	a.*,
                                    CASE
                                        WHEN  limite_pedido <= 50000 THEN '01 - Até 50K'
                                        WHEN  limite_pedido > 50000 AND  limite_pedido <= 60000 THEN '02 - 50K - 60K'
                                        WHEN  limite_pedido > 60000 AND  limite_pedido <= 70000 THEN '03 - 60K - 70K'
                                        WHEN  limite_pedido > 70000 AND  limite_pedido <= 80000 THEN '04 - 70K - 80K'
                                        WHEN  limite_pedido > 80000 AND  limite_pedido <= 90000 THEN '05 - 80K - 90K'
                                        WHEN  limite_pedido > 90000 AND  limite_pedido <= 100000 THEN '06 - 90K - 100K'
                                        WHEN  limite_pedido > 100000 AND  limite_pedido <= 110000 THEN '07 - 100K - 110K'
                                        WHEN  limite_pedido > 110000 AND  limite_pedido <= 120000 THEN '08 - 110K - 120K'
                                        WHEN  limite_pedido > 120000 AND  limite_pedido <= 130000 THEN '09 - 120K - 130K'
                                        WHEN  limite_pedido > 130000 AND  limite_pedido <= 140000 THEN '10 - 130K - 140K'
                                        WHEN  limite_pedido > 140000 AND  limite_pedido <= 150000 THEN '11 - 140K - 150K'
                                        ELSE '12 - >150K'
                                    END AS faixa_valor_solicitado,
                                    coalesce (b.vop, 0) as vop, coalesce (b.vencido, 0) as vencido, coalesce(b.vop_over_30, 0) as vop_over_30 
                            from deltalaketrusted.jira.propostas as a
                            left join ( select cnpj_sacado, SUM(vop) as vop, sum(vencido) as vencido, sum (vop_over_30) as vop_over_30
                                        from deltalakerefined.payments.vop_vendermais vv 
                                        where date(safra_concessao) >= date '2025-04-01'
                                        group by cnpj_sacado) as b
                            on a.cnpj = b.cnpj_sacado
                            where politica = 'DESAFIANTE'
                            and date(data_criado) >= date '2025-04-14') as sub
                            group by sub.faixa_valor_solicitado
                            order by sub.faixa_valor_solicitado
                    """
    
    
    # Verifica se a conexão foi bem-sucedida antes de executar a consulta reporte diário de propostas
    if conn is not None:
        propostas_desafiante = execute_query(conn, query_acomp_desafiante)
        propostas_desafiante_consolidado = execute_query(conn, query_acomp_desafiante_consolidado)
    else:
        propostas_desafiante = None
        propostas_desafiante_consolidado = None
        print("A consulta não foi executada porque a conexão com o Trino falhou.")



    # Cálculos dos percentuais reporte diário de propostas

    propostas_totais = propostas_desafiante['Entrantes'].sum()
    propostas_mesa = propostas_desafiante['Derivadas Mesa'].sum()
    propostas_reprov_motor = propostas_desafiante['Reprovadas Motor'].sum()
    propostas_aprov_motor = propostas_desafiante['Aprovadas Motor'].sum()

    if propostas_totais > 0:
        percent_mesa = (propostas_mesa / propostas_totais) * 100
        percent_reprov_motor = (propostas_reprov_motor / propostas_totais) * 100
        percent_aprov_motor = (propostas_aprov_motor / propostas_totais) * 100
    else:
        percent_mesa = 0
        percent_reprov_motor = 0
        percent_aprov_motor = 0

    print(percent_mesa)
    print(percent_reprov_motor)
    print(percent_aprov_motor)



    # Cálculos dos percentuais reporte consolidado de propostas

    propostas_totais_consolidado = propostas_desafiante_consolidado['Entrantes'].sum()
    propostas_mesa_consolidado = propostas_desafiante_consolidado['Derivadas Mesa'].sum()
    propostas_reprov_motor_consolidado = propostas_desafiante_consolidado['Reprovadas Motor'].sum()
    propostas_aprov_motor_consolidado = propostas_desafiante_consolidado['Aprovadas Motor'].sum()

    if propostas_totais_consolidado > 0:
        percent_mesa_consolidado = (propostas_mesa_consolidado / propostas_totais_consolidado) * 100
        percent_reprov_motor_consolidado = (propostas_reprov_motor_consolidado / propostas_totais_consolidado) * 100
        percent_aprov_motor_consolidado = (propostas_aprov_motor_consolidado / propostas_totais_consolidado) * 100
    else:
        percent_mesa_consolidado = 0
        percent_reprov_motor_consolidado = 0
        percent_aprov_motor_consolidado = 0

    print(percent_mesa_consolidado)
    print(percent_reprov_motor_consolidado)
    print(percent_aprov_motor_consolidado)



    print("Print do DF, antes da criação do markdown:")
    print(propostas_desafiante_consolidado)
    print(propostas_desafiante)



    #Adiciona totais no reporte diário de propostas
    totais = {
    'Faixa Valor Solicitado': 'Total',
    'Entrantes': propostas_desafiante['Entrantes'].sum(),
    'Derivadas Mesa': propostas_desafiante['Derivadas Mesa'].sum(),
    'Reprovadas Motor': propostas_desafiante['Reprovadas Motor'].sum(),
    'Aprovadas Motor': propostas_desafiante['Aprovadas Motor'].sum(),
    'Valor Aprovado Motor': propostas_desafiante['Valor Aprovado Motor'].sum()
    }

    # Adiciona linha de totais
    propostas_desafiante = pd.concat([propostas_desafiante, pd.DataFrame([totais])], ignore_index=True)

    propostas_desafiante['Valor Aprovado Motor'] = propostas_desafiante['Valor Aprovado Motor'].apply(
        lambda x: f"R$ {x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') if pd.notnull(x) and isinstance(x, (int, float)) else "R$ 0,00"
    )



    #Adiciona totais no reporte consolidado de propostas
    totais_consolidado = {
        'Faixa Valor Solicitado': 'Total',
        'Entrantes': propostas_desafiante_consolidado['Entrantes'].sum(),
        'Derivadas Mesa': propostas_desafiante_consolidado['Derivadas Mesa'].sum(),
        'Reprovadas Motor': propostas_desafiante_consolidado['Reprovadas Motor'].sum(),
        'Aprovadas Motor': propostas_desafiante_consolidado['Aprovadas Motor'].sum(),
        'Valor Aprovado Motor': propostas_desafiante_consolidado['Valor Aprovado Motor'].sum(),
        'VOP': propostas_desafiante_consolidado['VOP'].sum(),
        'Vencido': propostas_desafiante_consolidado['Vencido'].sum(),
        'Over 30': propostas_desafiante_consolidado['Over 30'].sum()

    }

    # Adiciona a linha de totais ao final da tabela consolidada
    propostas_desafiante_consolidado = pd.concat(
        [propostas_desafiante_consolidado, pd.DataFrame([totais_consolidado])],
        ignore_index=True
    )

    propostas_desafiante_consolidado['Valor Aprovado Motor'] = propostas_desafiante_consolidado['Valor Aprovado Motor'].apply(
        lambda x: f"R$ {x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') if pd.notnull(x) and isinstance(x, (int, float)) else "R$ 0,00"
    )

    propostas_desafiante_consolidado['VOP'] = propostas_desafiante_consolidado['VOP'].apply(
        lambda x: f"R$ {x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') if pd.notnull(x) and isinstance(x, (int, float)) else "R$ 0,00"
    )

    propostas_desafiante_consolidado['Vencido'] = propostas_desafiante_consolidado['Vencido'].apply(
        lambda x: f"R$ {x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') if pd.notnull(x) and isinstance(x, (int, float)) else "R$ 0,00"
    )

    propostas_desafiante_consolidado['Over 30'] = propostas_desafiante_consolidado['Over 30'].apply(
        lambda x: f"R$ {x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') if pd.notnull(x) and isinstance(x, (int, float)) else "R$ 0,00"
    )



    # Separar total - Diaria
    df_total = propostas_desafiante[propostas_desafiante['Faixa Valor Solicitado'].str.strip().str.lower() == 'total']
    df_main = propostas_desafiante[propostas_desafiante['Faixa Valor Solicitado'].str.strip().str.lower() != 'total']

    # Ordenar faixas
    propostas_desafiante_sorted = df_main.sort_values(by='Faixa Valor Solicitado', ascending=True)

    # Concatenar novamente com Total no final
    propostas_desafiante_sorted = pd.concat([propostas_desafiante_sorted, df_total], ignore_index=True)



    # Separar total - Consolidada
    df_total_consolidado = propostas_desafiante_consolidado[propostas_desafiante_consolidado['Faixa Valor Solicitado'].str.strip().str.lower() == 'total']
    df_main_consolidado = propostas_desafiante_consolidado[propostas_desafiante_consolidado['Faixa Valor Solicitado'].str.strip().str.lower() != 'total']

    # Ordenar faixas
    propostas_desafiante_consolidado_sorted = df_main_consolidado.sort_values(by='Faixa Valor Solicitado', ascending=True)

    # Concatenar novamente com Total no final
    propostas_desafiante_consolidado_sorted = pd.concat([propostas_desafiante_consolidado_sorted, df_total_consolidado], ignore_index=True)



    tabela_formatada_1 = tabulate(
        propostas_desafiante_sorted.values.tolist(),
        headers=propostas_desafiante_sorted.columns.tolist(),
        tablefmt="pretty"
    )

    tabela_formatada_2 = tabulate(
        propostas_desafiante_consolidado_sorted.values.tolist(),
        headers=propostas_desafiante_consolidado_sorted.columns.tolist(),
        tablefmt="pretty"
    )

    # Markdown invisível para espaçamento no Teams
    invisible_space = "\u200B"

    # Criar markdown final com percentuais separados por tabela
    markdown = (
        "📊 Resumo Diário de Propostas - Política Desafiante\n\n"
        f"{invisible_space}\n"
        f"📅 Data Referência: {data_execucao}\n\n"  # use sua variável de data aqui
        "---\n"
        f"🧾 % Derivadas para Mesa: {percent_mesa:.2f}%\n"
        f"✅ % Aprovadas Motor: {percent_aprov_motor:.2f}%\n"
        f"❌ % Reprovadas Motor: {percent_reprov_motor:.2f}%\n\n"
        "```\n" + tabela_formatada_1 + "\n```\n"
        "---\n"
        f"{invisible_space}\n"
        "---\n"
        "📊 Resumo Consolidado\n\n"
        f"🧾 % Derivadas para Mesa: {percent_mesa_consolidado:.2f}%\n"
        f"✅ % Aprovadas Motor: {percent_aprov_motor_consolidado:.2f}%\n"
        f"❌ % Reprovadas Motor: {percent_reprov_motor_consolidado:.2f}%\n\n"
        "```\n" + tabela_formatada_2 + "\n```"
    )

    # Exibir o markdown final
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