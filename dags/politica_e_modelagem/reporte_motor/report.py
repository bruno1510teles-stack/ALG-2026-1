import requests
from trino.dbapi import connect
from trino.auth import BasicAuthentication


def report_motor(access_params=None):
    def executar_query_trino_simples():
        query = """
        SELECT 
            cast(try(cast(data_resolvido as date)) as varchar) as data, 
            politica, 
            decisao, 
            count(*) as qtde
        FROM 
            deltalaketrusted.jira.propostas 
        WHERE 
            decisor = 'MOTOR' 
                AND try(cast(data_resolvido as date)) =
                CASE 
                    WHEN EXTRACT(DOW FROM current_date) = 1 THEN current_date - INTERVAL '3' DAY 
                    ELSE current_date - INTERVAL '1' DAY 
                END
        GROUP BY  
            cast(try(cast(data_resolvido as date)) as varchar), politica, decisao
        union all
        SELECT 
            'Total Geral' as data,
            '-' as politica, 
            '-' as decisao, 
            count(*) as qtde
        FROM 
            deltalaketrusted.jira.propostas 
        WHERE 
            decisor = 'MOTOR' 
            AND try(cast(data_resolvido as date)) =  
            CASE 
                WHEN EXTRACT(DOW FROM current_date) = 1 THEN current_date - INTERVAL '3' DAY 
                ELSE current_date - INTERVAL '1' DAY 
            END
        GROUP BY  
            cast(try(cast(data_resolvido as date)) as varchar)
        order by
            data
        """
        # Executar a query no Trino
        conn = connect(
            host=access_params['trino_endpoint'],
            port=access_params['trino_port'],
            user=access_params['trino_user'],
            auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
            http_scheme="https",
        )
        cur = conn.cursor()
        cur.execute(query)
        resultados = cur.fetchall()
        return resultados

    def executar_query_trino_detalhada():
        query = """
        SELECT 
            cast(try(cast(data_resolvido as date)) as varchar) as data,  
            politica, 
            decisao, 
            ramificacao_motor, 
            parecer, 
            count(*) as qtde,
            min(issue_key) as amostra_1,
            max(issue_key) as amostra_2
        FROM 
            deltalaketrusted.jira.propostas 
        WHERE 
            decisor = 'MOTOR' 
                AND try(cast(data_resolvido as date)) = 
                CASE 
                    WHEN EXTRACT(DOW FROM current_date) = 1 THEN current_date - INTERVAL '3' DAY 
                    ELSE current_date - INTERVAL '1' DAY 
                END
        GROUP BY  
            cast(try(cast(data_resolvido as date)) as varchar), politica, decisao, ramificacao_motor, parecer
        union all
        SELECT 
            'Total Geral' as data, 
            '-' as politica, 
            '-' as decisao, 
            '-' as ramificacao_motor, 
            '-' as parecer, 
            count(*) as qtde,
            '-' as amostra_1,
            '-' as amostra_2
        FROM 
            deltalaketrusted.jira.propostas 
        WHERE 
            decisor = 'MOTOR' 
                AND try(cast(data_resolvido as date)) = 
                CASE 
                    WHEN EXTRACT(DOW FROM current_date) = 1 THEN current_date - INTERVAL '3' DAY 
                    ELSE current_date - INTERVAL '1' DAY 
                END
        GROUP BY  
            cast(try(cast(data_resolvido as date)) as varchar)
        order by
            qtde desc
        """
        # Executar a query no Trino
        conn = connect(
            host=access_params['trino_endpoint'],
            port=access_params['trino_port'],
            user=access_params['trino_user'],
            auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
            http_scheme="https",
        )
        cur = conn.cursor()
        cur.execute(query)
        resultados = cur.fetchall()
        return resultados

    # Função para formatar os resultados da query simples
    def formatar_mensagem_simples(resultados):
        mensagem = "\n### Relatório Resumido de Decisões do Motor de Crédito (último dia útil)\n"
        mensagem += "| Data       | Política  | Decisão | Quantidade |\n"
        mensagem += "|------------|-----------|---------|------------|\n"
        
        for row in resultados:
            mensagem += f"| {row[0]} | {row[1]}   | {row[2]}   | {row[3]}        |\n"
        
        return mensagem

    # Função para formatar os resultados da query detalhada
    def formatar_mensagem_detalhada(resultados):
        mensagem = "\n### Relatório Detalhado de Decisões do Motor de Crédito (último dia útil)\n"
        mensagem += "| Data       | Política  | Decisão | Ramificação | Parecer | Quantidade | Amostra 1 | Amostra 2 |\n"
        mensagem += "|------------|-----------|---------|-------------|---------|------------|-----------|-----------|\n"
        
        for row in resultados:
            mensagem += f"| {row[0]} | {row[1]}   | {row[2]}   | {row[3]}   | {row[4]}   | {row[5]}        | {row[6]}        | {row[7]}        |\n"
        
        return mensagem

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

    # Executar o fluxo completo
    resultados_simples = executar_query_trino_simples()
    resultados_detalhados = executar_query_trino_detalhada()

    # Formatar as duas mensagens
    mensagem_simples = formatar_mensagem_simples(resultados_simples)
    mensagem_detalhada = formatar_mensagem_detalhada(resultados_detalhados)

    # Combinar as mensagens
    mensagem_final = mensagem_simples + "\n" + mensagem_detalhada

    # Enviar a mensagem combinada para o webhook
    enviar_para_webhook(mensagem_final)