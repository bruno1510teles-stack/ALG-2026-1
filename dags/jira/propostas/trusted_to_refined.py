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
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, time, timedelta, timezone


def trusted_to_refined (access_params=None, **kwargs):


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


    # LOGICA ANALISTA RESPONSAVEL, PRIMEIRO ANALISTA QUE APARECE NA PRIMEIRA PROPOSTA DO CLIENTE

    # Convertendo a coluna de data para datetime
    df['data_resolvido'] = pd.to_datetime(df['data_resolvido'])

    # Filtrando as linhas onde a decisão é "APROVADO"
    df_aprovado = df[df['decisao'] == 'APROVADO']

    # Ordenando o DataFrame pela raiz_cnpj e data_resolvido
    df_sorted = df_aprovado.sort_values(by=['raiz_cnpj', 'data_resolvido'], ascending=[True, True])

    # Atribuindo o analista responsável, pegando o primeiro analista de cada grupo de raiz_cnpj
    df['analista_responsavel'] = df_sorted.groupby('raiz_cnpj')['analista_tratado'].transform('first')

    # Preenchendo com 'NA' para as linhas que não têm analista responsável (onde for NaN)
    df['analista_responsavel'] = df['analista_responsavel'].fillna('NA')


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


    # SLA DE DECISÃO

    # Função que calcula horas úteis entre duas datas
    def calcular_tempo_util(inicio_str, fim_str):
        if pd.isna(inicio_str) or pd.isna(fim_str):
            return None

        inicio = pd.to_datetime(inicio_str)
        fim = pd.to_datetime(fim_str)

        if inicio.tzinfo is not None and fim.tzinfo is not None:
            inicio = inicio.tz_convert(None)
            fim = fim.tz_convert(None)

        hora_trabalho_inicio = time(7, 0)
        hora_trabalho_fim = time(20, 0)

        if inicio > fim:
            inicio, fim = fim, inicio

        dias_uteis = pd.date_range(inicio.date(), fim.date(), freq='B')
        total_horas = 0

        for dia in dias_uteis:
            dia_data = dia.date()
            inicio_dia = datetime.combine(dia_data, hora_trabalho_inicio)
            fim_dia = datetime.combine(dia_data, hora_trabalho_fim)

            if dia_data == inicio.date():
                inicio_dia = max(inicio, inicio_dia)
            if dia_data == fim.date():
                fim_dia = min(fim, fim_dia)

            if inicio_dia < fim_dia:
                diff = (fim_dia - inicio_dia).total_seconds() / 3600
                total_horas += diff

        return round(total_horas, 2)
    

    # Função para contar dias úteis entre duas datas
    def calcular_dias_uteis(inicio, fim):
        if pd.isna(inicio) or pd.isna(fim):
            return None
        inicio = pd.to_datetime(inicio)
        fim = pd.to_datetime(fim)
        if inicio > fim:
            inicio, fim = fim, inicio
        return len(pd.bdate_range(inicio.date(), fim.date())) - 1  # exclui o dia de início


    def classificar_sla_horas(diferenca_horas):
        if pd.isna(diferenca_horas):
            return "SLA NÃO DEFINIDO"
        diferenca_horas = round(diferenca_horas, 2)
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
        if pd.isna(dias):
            return "SLA NÃO DEFINIDO"
        elif dias <= 0:
            return "01 - D = 0"
        elif dias == 1:
            return "02 - D + 1"
        elif dias == 2:
            return "03 - D + 2"
        elif dias >= 3:
            return "04 - >= D + 3"
        
    
    # Adiciona colunas de atualização
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df['year'], df['month'], df['day'] = now.year, now.month, now.day

    # Cálculo de horas e dias úteis
    df['diferenca_horas_criado_resolvido'] = df.apply(lambda row: calcular_tempo_util(row['data_criado'], row['data_resolvido']), axis=1)
    df['diferenca_dias_criado_resolvido'] = df.apply(lambda row: calcular_dias_uteis(row['data_criado'], row['data_resolvido']), axis=1)

    df['diferenca_horas_disp_mesa_resolvido'] = df.apply(lambda row: calcular_tempo_util(row['data_disponivel_mesa'], row['data_resolvido']), axis=1)
    df['diferenca_dias_disp_mesa_resolvido'] = df.apply(lambda row: calcular_dias_uteis(row['data_disponivel_mesa'], row['data_resolvido']), axis=1)

    # Classificações de SLA baseadas em horas e dias úteis
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
        'limite_aprovado', 'gerente_tratado', 'nome_vendedor_fn', 'filial_fn',
        'prioridade', 'status','categoria_decisor', 'decisao',
        'parecer', 'ramificacao_motor', 'tipo_proposta', 'data_criado',
        'data_resolvido', 'data_atualizado', 'data_disponivel_mesa',
        'diferenca_dias_criado_resolvido', 'diferenca_horas_criado_resolvido',
        'diferenca_dias_disp_mesa_resolvido',
        'diferenca_horas_disp_mesa_resolvido', 'sla_hora_criado_resolvido',
        'sla_hora_disp_mesa_resolvido', 'sla_dias_criado_resolvido',
        'sla_dias_disp_mesa_resolvido', 'faixa_valor_solicitado',
        'aprovacao_percent', 'status_aprovacao_percent', 'status_relacional','analista_tratado',
        'analista_responsavel', 'atualizado_em', 'year', 'month', 'day',
        ]
    ].reset_index(drop=True)


    # Configurações para acesso ao MinIO
    logger = LoggingMixin().log 

    try:
        logger.info("Iniciando salvamento das informações")
        
        storage_options = {
            "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
            "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
            "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_refined']}",
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