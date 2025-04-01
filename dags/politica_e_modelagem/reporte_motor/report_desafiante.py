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


def report_motor_desafiante (access_params=None):

    # Conexão com o banco de dados
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
    

    query_propostas =   f"""
                        select *
                        from deltalaketrusted.jira.propostas
                        where politica = 'DESAFIANTE'
                    """
    

    if conn is not None:
        propostas = execute_query(conn, query_propostas)
        print("Dados carregados com sucesso.")
    else:
        print("A consulta não foi executada porque a conexão com o Trino falhou.")

    
    ## PROPOSTAS APROVADAS PELO MOTOR - DESAFIANTE
    aprovadas_motor_autom = propostas[propostas['parecer'] == 'Motor - Aprovado']

    aprovadas_motor_autom_len = len(aprovadas_motor_autom)

    print('Propostas aprovadas pelo motor Desafiante:')
    print(aprovadas_motor_autom_len)



    ## PROPOSTAS APROVADAS PELO MOTOR >300K LIM SOLICITADO - ENVIADO MESA
    aprovadas_motor_env_mesa_300k = propostas[
        propostas['ramificacao_motor'].isin(['A6 | A1', 'A7 | A2', 'A8 | A3', 'AA | AA', 
                                            'A1 | A1', 'A2 | A2', 'A3 | A1', 'A4 | A2', 
                                            'A5 | A3']) &
        (propostas['limite_pedido'] > 300000)
    ]

    aprovadas_motor_env_mesa_300k_len = len(aprovadas_motor_env_mesa_300k)

    print('Propostas aprovadas pelo motor, porém enviadas para a mesa por conta do valor (>300k lim solicitado):')
    print(aprovadas_motor_env_mesa_300k_len)

    ## -----------------------------------------------------------------------------------------------------------------------------------------

    aprovadas_motor_env_mesa_300k_decisao = propostas[
        propostas['ramificacao_motor'].isin(['A6 | A1', 'A7 | A2', 'A8 | A3', 'AA | AA', 
                                            'A1 | A1', 'A2 | A2', 'A3 | A1', 'A4 | A2', 
                                            'A5 | A3']) &
        (propostas['limite_pedido'] > 300000) &
        (propostas['categoria_decisor'] == 'MESA') &
        (propostas['decisao'] == 'APROVADO')
    ]

    aprovadas_motor_env_mesa_300k_decisao_len = len(aprovadas_motor_env_mesa_300k_decisao)

    print('Aprovadas pelas mesa após análise prévia do motor:')
    print(aprovadas_motor_env_mesa_300k_decisao_len)



    ## PROPOSTAS APROVADAS PELO MOTOR, PORÉM SEM ALÇADA (>50K e <300k) - SEM PONTUALIDADE
    aprovadas_motor_sem_hp_50k_300k_mesa = propostas[
        propostas['ramificacao_motor'].isin(['A6 | A1', 'A7 | A2', 'A8 | A3']) &
        (propostas['limite_pedido'] <= 300000) &
        (propostas['limite_pedido'] >= 50000) &
        (propostas['parecer'] != 'Motor - Aprovado')
    ]

    aprovadas_motor_sem_hp_50k_300k_mesa_len = len(aprovadas_motor_sem_hp_50k_300k_mesa)

    print('Propostas aprovadas pelo motor, porém enviadas para a mesa por conta do valor (>50k e <300k lim solicitado):')
    print(aprovadas_motor_sem_hp_50k_300k_mesa_len)

    ## -----------------------------------------------------------------------------------------------------------------------------------------

    aprovadas_motor_sem_hp_50k_300k_mesa_decisao = propostas[
        propostas['ramificacao_motor'].isin(['A6 | A1', 'A7 | A2', 'A8 | A3']) &
        (propostas['limite_pedido'] <= 300000) &
        (propostas['limite_pedido'] >= 50000) &
        (propostas['parecer'] != 'Motor - Aprovado') &
        (propostas['categoria_decisor'] == 'MESA') &
        (propostas['decisao'] == 'APROVADO')
    ]

    aprovadas_motor_sem_hp_50k_300k_mesa_decisao_len = len(aprovadas_motor_sem_hp_50k_300k_mesa_decisao)

    print('Aprovadas pelas mesa após análise prévia do motor:')
    print(aprovadas_motor_sem_hp_50k_300k_mesa_decisao_len)



    ## PROPOSTAS APROVADAS PELO MOTOR, PORÉM SEM ALÇADA (>50K e <300k) - COM PONTUALIDADE

    aprovadas_motor_com_hp_50k_300k_mesa = propostas[
        propostas['ramificacao_motor'].isin(['AA | AA', 'A1 | A1', 'A2 | A2', 'A3 | A1', 'A4 | A2', 'A5 | A3']) &
        (propostas['limite_pedido'] <= 300000) &
        (propostas['limite_pedido'] >= 50000) &
        (propostas['parecer'] != 'Motor - Aprovado')
    ]

    aprovadas_motor_com_hp_50k_300k_mesa_len = len(aprovadas_motor_com_hp_50k_300k_mesa)

    print('Propostas aprovadas pelo motor, porém enviadas para a mesa por conta do valor (>50k e <300k lim solicitado):')
    print(aprovadas_motor_com_hp_50k_300k_mesa_len)

    ## -----------------------------------------------------------------------------------------------------------------------------------------

    aprovadas_motor_com_hp_50k_300k_mesa_decisao = propostas[
        propostas['ramificacao_motor'].isin(['AA | AA', 'A1 | A1', 'A2 | A2', 'A3 | A1', 'A4 | A2', 'A5 | A3']) &
        (propostas['limite_pedido'] <= 300000) &
        (propostas['limite_pedido'] >= 50000) &
        (propostas['parecer'] != 'Motor - Aprovado') &
        (propostas['categoria_decisor'] == 'MESA') &
        (propostas['decisao'] == 'APROVADO')
    ]

    aprovadas_motor_com_hp_50k_300k_mesa_decisao_len = len(aprovadas_motor_com_hp_50k_300k_mesa_decisao)

    print('Aprovadas pelas mesa após análise prévia do motor:')
    print(aprovadas_motor_com_hp_50k_300k_mesa_decisao_len)


    ## PROPOSTAS APROVADAS PELA MESA, COM RAMIFICAÇÕES DIFERENTES DE APROVADAS NO MOTOR
    aprovadas_outras_ramificacoes = propostas[
        ~propostas['ramificacao_motor'].isin(['A6 | A1', 'A7 | A2', 'A8 | A3', 'AA | AA', 
                                            'A1 | A1', 'A2 | A2', 'A3 | A1', 'A4 | A2', 
                                            'A5 | A3']) &
        (propostas['decisao'] == 'APROVADO')
    ]

    aprovadas_outras_ramificacoes_len = len(aprovadas_outras_ramificacoes)

    print('Propostas aprovadas pela mesa, que o motor não aprovaria:')
    print(aprovadas_outras_ramificacoes_len)


    ## APROVAÇÕES TOTAIS NA DESAFIANTE
    aprovadas_totais_motor = propostas[
        propostas['ramificacao_motor'].isin(['A6 | A1', 'A7 | A2', 'A8 | A3', 'AA | AA', 
                                            'A1 | A1', 'A2 | A2', 'A3 | A1', 'A4 | A2', 
                                            'A5 | A3']) &
        (propostas['decisao'] == 'APROVADO')
    ]

    aprovadas_totais_motor_len = len(aprovadas_totais_motor)

    print('Propostas aprovadas totais motor:')
    print(aprovadas_totais_motor_len)



    def calcular_percentual(aprovadas_len, aprovadas_decisao_len):
        if aprovadas_len == 0:
            return 0  # Se o denominador for zero, retornamos 0%
        return (aprovadas_decisao_len / aprovadas_len) * 100

    percent_aprov_300k_mesa = calcular_percentual(aprovadas_motor_env_mesa_300k_len, aprovadas_motor_env_mesa_300k_decisao_len)
    percent_aprov_sem_alcada_sem_hp = calcular_percentual(aprovadas_motor_sem_hp_50k_300k_mesa_len, aprovadas_motor_sem_hp_50k_300k_mesa_decisao_len)
    percent_aprov_sem_alcada_com_hp = calcular_percentual(aprovadas_motor_com_hp_50k_300k_mesa_len, aprovadas_motor_com_hp_50k_300k_mesa_decisao_len)

    percent_aprov_mesa_total = ((aprovadas_motor_env_mesa_300k_decisao_len + 
                                aprovadas_motor_sem_hp_50k_300k_mesa_decisao_len +
                                aprovadas_motor_com_hp_50k_300k_mesa_decisao_len) / aprovadas_totais_motor_len) * 100
        

    # Imprimir os resultados
    print(f"Quantidade Propostas 300K Mesa : {aprovadas_motor_env_mesa_300k_decisao_len}")
    print(f"Percentual 300K Mesa: {percent_aprov_300k_mesa}%")
    print(f"Quantidade Propostas Sem Alçada Sem HP : {aprovadas_motor_sem_hp_50k_300k_mesa_len}")
    print(f"Percentual Sem Alçada Sem HP: {percent_aprov_sem_alcada_sem_hp}%")
    print(f"Quantidade Propostas Sem Alçada Cem HP : {aprovadas_motor_com_hp_50k_300k_mesa_len}")
    print(f"Percentual Sem Alçada Com HP: {percent_aprov_sem_alcada_com_hp}%")


    # Função para formatar os resultados detalhados no formato desejado
    def formatar_mensagem_detalhada(resultados):
        # Cabeçalho da tabela
        mensagem = "\n### RELATÓRIO DE APROVAÇÃO MOTOR DESAFIANTE\n"
        mensagem += "| Descrição                                     | Propostas | (%) Aprovação Mesa |\n"
        mensagem += "|----------------------------------------------|-----------|--------------------|\n"

        # Adicionando os dados à tabela com alinhamento
        for row in resultados:
            mensagem += f"| {row[0]:<45} | {row[1]:>10} | {row[2]:>17} |\n"
        
        return mensagem

    # Simulação de dados de resultados
    resultados = [
        ("Aprovadas pelo Motor", aprovadas_motor_autom_len, "-"), 
        ("Aprovadas, porém sem alçada (>300K)", aprovadas_motor_env_mesa_300k_decisao_len, f"{percent_aprov_300k_mesa:.1f}%"),
        ("Aprovadas, porém sem alçada (>50K) e SEM HP", aprovadas_motor_sem_hp_50k_300k_mesa_len, f"{percent_aprov_sem_alcada_sem_hp:.1f}%"),
        ("Aprovadas, porém sem alçada (>50K) e COM HP", aprovadas_motor_com_hp_50k_300k_mesa_len, f"{percent_aprov_sem_alcada_com_hp:.1f}%"),
        ("Total", aprovadas_totais_motor_len, f"{percent_aprov_mesa_total:.1f}%")
    ]

    # Gerando a tabela formatada
    tabela_detalhada = formatar_mensagem_detalhada(resultados)

    # Imprimir a tabela (para visualizar antes de enviar)
    print(tabela_detalhada)


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
    enviar_para_webhook(tabela_detalhada)


