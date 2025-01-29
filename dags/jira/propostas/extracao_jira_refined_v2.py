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


def jira_trusted_to_refined(access_params=None, **kwargs):


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


    # Inspecionando colunas do DataFrame
    print("Colunas carregadas e disponíveis no DataFrame:")
    print(df.columns.tolist())


    # Criando Funções para formatar DataFrame
    print("CRIANDO FUNÇÕES PARA FORMATAR DATAFRAME...")

    def format_faixa_valor_solicitado(vlr):
        if pd.isnull(vlr) or not isinstance(vlr, (int, float)):
            return "VALOR INVALIDO"
        
        if vlr <= 30000:
            return "01 - ATE R$30.000"
        elif 30000 < vlr <= 50000:
            return "02 - R$30.000 A R$50.000"
        elif 50000 < vlr <= 80000:
            return "03 - R$50.000 A R$80.000"
        elif 80000 < vlr <= 120000:
            return "04 - R$80.000 A R$120.000"
        elif 120000 < vlr <= 200000:
            return "05 - R$120.000 A R$200.000"
        elif 200000 < vlr <= 250000:
            return "06 - R$200.000 A R$250.000"
        elif 250000 < vlr <= 400000:
            return "07 - R$250.000 A R$400.000"
        elif 400000 < vlr <= 500000:
            return "08 - R$400.000 A R$500.000"
        elif 500000 < vlr <= 1000000:
            return "09 - R$500.000 A R$1.000.000"
        elif 1000000 < vlr <= 5000000:
            return "10 - R$1.000.000 A R$5.000.000"
        elif vlr > 5000000:
            return "11 - MAIOR QUE R$5.000.000"

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
            return "05 - > 100%"
        else:
            return "VALOR NÃO INFORMADO"


    def status_decisao_relacional(status_decisao, status_aprovacao):
        if status_decisao == "REPROVADO":
            return "REPROVADO"
        elif status_aprovacao == "01 - 0-50%":
            return "APROVADO COM REDUÇÃO"
        elif status_aprovacao == "02 - 50-75%":
            return "APROVADO COM REDUÇÃO"
        elif status_aprovacao == "03 - 75-100%":
            return "APROVADO COM REDUÇÃO"
        elif status_aprovacao == "04 - 100%":
            return "APROVADO"
        elif status_decisao == "APROVADO":
            return "APROVADO"
        else:
            return "NÃO ATRIBUIDA"
        
    def classificar_sla_horas(diferenca_horas):
        if pd.isna(diferenca_horas):  # Verifica se o valor é NaN
            return "SLA NÃO DEFINIDO"

        diferenca_horas = round(diferenca_horas, 2)  # Arredondar para 2 casas decimais
        
        if diferenca_horas <= 2:
            return "01 - ATE 2 HORAS"
        elif diferenca_horas <= 4:
            return "02 - 2-4 HORAS"
        elif diferenca_horas <= 8:
            return "03 - 4-8 HORAS"
        elif diferenca_horas <= 24:
            return "04 - 8-24 HORAS"
        elif diferenca_horas <= 48:
            return "05 - D + 1"
        elif diferenca_horas <= 72:
            return "06 - D + 2"
        return "07 - D + 3"
        
        
    def classificar_sla_dias(dias):
        if pd.isna(dias):  # Verifica se o valor é NaN
            return "SLA NÃO DEFINIDO"
        elif dias == 0:
            return "01 - D = 0"
        elif dias == 1:
            return "02 - D + 1"
        elif dias == 2:
            return "03 - D + 2"
        elif dias >= 3:
            return "04 - >= D + 3"
        
    
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df['year'], df['month'], df['day'] = now.year, now.month, now.day


    # Criando df para verificar a diferença de dias
    df['diferenca_dias_criado_resolvido'] = ( df['data_resolvido'] - df['data_criado']).dt.days.astype('Int64')
    df['diferenca_horas_criado_resolvido'] = ( df['data_resolvido'] - df['data_criado']).dt.total_seconds() / 3600

    df['diferenca_dias_disp_mesa_resolvido'] = ( df['data_resolvido'] - df['data_disponivel_mesa']).dt.days.astype('Int64')
    df['diferenca_horas_disp_mesa_resolvido'] = ( df['data_resolvido'] - df['data_disponivel_mesa']).dt.total_seconds() / 3600


    # SLA Tempo Dias e Horas
    df['sla_hora_criado_resolvido'] = df["diferenca_horas_criado_resolvido"].apply(classificar_sla_horas)
    df['sla_hora_disp_mesa_resolvido'] = df["diferenca_horas_disp_mesa_resolvido"].apply(classificar_sla_horas)

    df['sla_dias_criado_resolvido'] = df["diferenca_dias_criado_resolvido"].apply(classificar_sla_dias)
    df['sla_dias_disp_mesa_resolvido'] = df["diferenca_dias_disp_mesa_resolvido"].apply(classificar_sla_dias)


    # Aplicando funções para criar colunas com informações formatadas
    df['faixa_valor_solicitado'] = df['limite_pedido'].apply(format_faixa_valor_solicitado)


    # Criando df de Aprovação %
    df['aprovacao_percent'] = np.where(df['limite_pedido'] != 0, 
                            df['limite_aprovado'] / df['limite_pedido'], 
                            0)

    # Criando df Status Aprovação
    df['status_aprovacao_percent'] = df['aprovacao_percent'].apply(format_faixa_aprov)

    df['status_relacional'] = df.apply(
        lambda row: status_decisao_relacional(row['status'], row['status_aprovacao_percent']), axis=1
    )


    # Selecionando as colunas relevantes
    df_final = df[
        ['issue_key', 'politica', 'cnpj', 'raiz_cnpj', 'pgid', 'limite_pedido',
        'limite_aprovado', 'nome_vendedor_alpe_tratado', 'nome_vendedor_fn', 'filial_fn',
        'prioridade', 'status','categoria_decisor', 'decisao',
        'parecer', 'ramificacao_motor', 'tipo_proposta', 'data_criado',
        'data_resolvido', 'data_atualizado', 'data_disponivel_mesa',
        'diferenca_dias_criado_resolvido', 'diferenca_horas_criado_resolvido',
        'diferenca_dias_disp_mesa_resolvido',
        'diferenca_horas_disp_mesa_resolvido', 'sla_hora_criado_resolvido',
        'sla_hora_disp_mesa_resolvido', 'sla_dias_criado_resolvido',
        'sla_dias_disp_mesa_resolvido', 'faixa_valor_solicitado',
        'aprovacao_percent', 'status_aprovacao_percent', 'status_relacional',
        'atualizado_em', 'year', 'month', 'day',
        ]
    ].reset_index(drop=True)


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
            df_final, 
            partition_by=["year", "month", "day"],
            storage_options=storage_options,
            mode="overwrite"
            # overwrite_schema=True
    )
        logger.info("Salvamento concluído com sucesso.")
        
    except Exception as e:
        logger.error(f"Erro ao salvar as informações: {str(e)}")