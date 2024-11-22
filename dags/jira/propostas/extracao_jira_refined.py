# Importando Bibliotecas
import requests
import pandas as pd
import numpy as np
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

def base_details_refined(access_params=None,  **kwargs):

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
    query_jira_trusted = """
        SELECT *
        FROM deltalaketrusted.jira.propostas
    """

    # Verifica se a conexão foi bem-sucedida antes de executar a consulta
    if conn is not None:
        df = execute_query(conn, query_jira_trusted)
    else:
        df = None
        print("A consulta não foi executada porque a conexão com o Trino falhou.")

        return df
    

    # Inspecionando colunas do DataFrame
    print("Colunas carregadas e disponíveis no DataFrame:")
    print(df.columns.tolist())
    
    
    
    # Criando Funções para formatar DataFrame
    print("Criando funções para tratamento de colunas no DataFrame...")

    def format_faixa_valor_solicitado(vlr):
        if vlr <= 30000:
            return "01 - Até R$30.000"
        elif 30000 < vlr <= 50000:
            return "02 - R$30.000 a R$50.000"
        elif 50000 < vlr <= 80000:
            return "03 - R$50.000 a R$80.000"
        elif 80000 < vlr <= 120000:
            return "04 - R$80.000 a R$120.000"
        elif 120000 < vlr <= 200000:
            return "05 - R$120.000 a R$200.000"
        elif 200000 < vlr <= 250000:
            return "06 - R$200.000 a R$250.000"
        elif 250000 < vlr <= 400000:
            return "07 - R$250.000 a R$400.000"
        elif 400000 < vlr <= 500000:
            return "08 - R$400.000 a R$500.000"
        elif 500000 < vlr <= 1000000:
            return "09 - R$500.000 a R$1.000.000"
        elif 1000000 < vlr <= 5000000:
            return "10 - R$1.000.000 a R$5.000.000"
        elif vlr > 5000000:
            return "11 - Maior que R$5.000.000"

    def format_faixa_aprov(percent):
        if percent <= 0.5:
            return "01 - 0-50%"
        elif 0.5 < percent <= 0.75:
            return "02 - 50-75%"
        elif 0.75 < percent < 1:
            return "03 - 75-100%"
        elif percent == 1:
            return "04 - 100%"
        elif percent > 1:
            return "Aprovado acima do Valor Solicitado"
        else:
            return "Valor não especificado"


    def status_decisao_relacional(status_decisao, status_aprovacao):
        if status_decisao == "Reprovado":
            return "Reprovado"
        elif status_aprovacao == "01 - 0-50%":
            return "Aprovado com Redução"
        elif status_aprovacao == "02 - 50-75%":
            return "Aprovado com Redução"
        elif status_aprovacao == "03 - 75-100%":
            return "Aprovado com Redução"
        elif status_aprovacao == "04 - 100%":
            return "Aprovado"
        elif status_decisao == "Aprovado":
            return "Aprovado"
        else:
            return "Decisão não atribuída"

    def format_tipo_analista(nome_decisor):
        # Mapeamento de nomes para tipos
        tipo_mapping = {
            "Ana Beatriz Rodrigues Andrade": "Mesa",
            "Beatriz Pereira Gama Cardoso": "Outros",
            "Camila Mamede Cabral": "Outros",
            "Caroline Freihat Henrique De Alcantara Santana": "Mesa",
            "Claudia Cinare Rodrigues Eto": "Mesa",
            "Claudia Cravo": "Outros",
            "Decisor não atribuído": "Decisor não atribuído",
            "Diana Tiemi Yamamoto": "Mesa",
            "Jose Carvalho": "Mesa",
            "Larissa Freire Soares": "Mesa",
            "Leandro Quintino Da Anunciacao": "Mesa",
            "Mayara Costa": "Outros",
            "Motor": "Motor",
            "Priscila Yuri Nagata Ortega": "Outros",
            "Rafael Rocha Leite": "Outros",
            "Rogerio De Campos Frias": "Mesa",
            "Rosemeire Dias Ferreira": "Mesa",
            "Vanessa Souza": "Mesa",
            "Vivian Pompeu": "Outros"
        }
        
        # Retornar o tipo correspondente ou "Outros" se não estiver no mapeamento
        return tipo_mapping.get(nome_decisor, "Outros")
        
    print("Criação de funções finalizadas com sucesso!")
        
    print("Tratando colunas no DataFrame conforme as funções criadas...")
    # Adicionando colunas de data e hora
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df['year'], df['month'], df['day'] = now.year, now.month, now.day


    # Criando df para verificar a diferença de dias
    df['diferença_dias_criado_resolvido'] = (df['resolvido_tratado'] - df['criado_tratado']).dt.days.astype('Int64')
    df['diferença_dias_atribuido_resolvido'] = (df['atribuido_tratado'] - df['criado_tratado']).dt.days.astype('Int64')

    # Criando df para verificar a diferença de horas e minutos - Criado/ Resolvido
    df['diferença_horas_criado_resolvido'] = (df['resolvido_tratado'] - df['criado_tratado']).dt.total_seconds() / 3600
    df['diferença_minutos_criado_resolvido'] = (df['resolvido_tratado'] - df['criado_tratado']).dt.total_seconds() / 60

    # Criando df para verificar a diferença de horas e minutos - Atribuido/ Resolvido
    df['diferença_horas_atribuido_resolvido'] = (df['atribuido_tratado'] - df['resolvido_tratado']).dt.total_seconds() / 3600
    df['diferença_minutos_atribuido_resolvido'] = (df['atribuido_tratado'] - df['resolvido_tratado']).dt.total_seconds() / 60

    # Aplicando funções para criar colunas com informações formatadas
    df['faixa_valor_solicitado'] = df['limite_pedido'].apply(format_faixa_valor_solicitado)
    df['cnpj_formatado'] = df['cnpj'].str[:2] + '.' + df['cnpj'].str[2:5] + '.' + df['cnpj'].str[5:8] + '/' + df['cnpj'].str[8:12] + '-' + df['cnpj'].str[12:]

    # Criando df de Aprovação %
    df['aprovacao_percent'] = np.where(df['limite_pedido'] != 0, 
                            df['limite_aprovado'] / df['limite_pedido'], 
                            0)

    # Criando df Status Aprovação
    df['status_aprovacao_percent'] = df['aprovacao_percent'].apply(format_faixa_aprov)
    df['status_relacional'] = df.apply(
        lambda row: status_decisao_relacional(row['status_decisao'], row['status_aprovacao_percent']), axis=1
    )
    df['tipo_analista'] = df['nome_decisor'].apply(format_tipo_analista)

    print("Tratamento de DataFrame realizado com sucesso!")


    # Selecionando as colunas relevantes
    jira_tratado_refined = df[
        [
        'issue_key', 'politica_desc', 'cnpj', 'cnpj_formatado','pgid', 'limite_pedido', 'limite_aprovado', 'nome_issue',
        'tipo_proposta', 'vendedor_alpe', 'vendedor_fornecedor', 'filial_fornecedor',
        'tipo_status','nome_decisor', 'status_decisao','parecer_desc', 'ramificacao_motor_desc', 
        'data_criado', 'hora_criado', 
        'data_resolvido', 'hora_resolvido',
        'data_atribuido', 'hora_atribuido',
        'faixa_valor_solicitado', 
        'diferença_dias_criado_resolvido', 'diferença_dias_atribuido_resolvido', 
        'diferença_minutos_criado_resolvido', 'diferença_horas_criado_resolvido',
        'diferença_horas_atribuido_resolvido', 'diferença_minutos_atribuido_resolvido',
        'aprovacao_percent', 'status_aprovacao_percent',
        'status_relacional','tipo_analista',
        'atualizado_em', 'year', 'month', 'day'
        ]
    ].reset_index(drop=True)

    # Exibindo o DataFrame tratado
    print(f"{len(jira_tratado_refined)} propostas válidas.")
    print(jira_tratado_refined.head())


    # Configurações para acesso ao MinIO
    logger = LoggingMixin().log 
    
    try:
        logger.info("Iniciando salvamento das informações")
        
        storage_options = {
            "AWS_ACCESS_KEY_ID": "d8jOuN46ckGNsr6zzpyw",
            "AWS_SECRET_ACCESS_KEY": "gnyBdrsoDrRcM9ln0QO83Nw8I4TlOFDOI4J9QDKc",
            "AWS_ENDPOINT_URL": "https://api-refined.alpe.com.br",
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
        }


        # Definindo o caminho e salvando no MinIO
        BUCKET_SOURCE_REFINED = "jira"
        FOLDER_DESTINATION_REFINED = "propostas"

        write_deltalake(
            f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}", 
            jira_tratado_refined, 
            partition_by=["year", "month", "day"],
            storage_options=storage_options,
            mode="overwrite"
            # overwrite_schema=True
    )
        logger.info("Salvamento concluído com sucesso.")
        
    except Exception as e:
        logger.error(f"Erro ao salvar as informações: {str(e)}")