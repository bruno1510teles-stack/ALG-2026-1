# Importando Libs
import requests
import pandas as pd
import numpy as np
import base64
from minio import Minio
from io import BytesIO
from requests.auth import HTTPBasicAuth
import json
from airflow.utils.log.logging_mixin import LoggingMixin
from deltalake import write_deltalake, DeltaTable
from datetime import datetime, timezone, timedelta


def jira_raw_to_trusted(access_params=None, **kwargs):

    # Conectando no MinIO
    client = Minio(
        "api-raw.alpe.com.br",
        access_key = 'B7q0avvSIpSdyGPXWnEC',
        secret_key = 'PhMhRQSQ6YJU8fn2qKhDLM017cQPrlCz1YbM8IwU'
    )

    # Definindo bucket e caminho do arquivo
    BUCKET_SOURCE_RAW = "jira"
    FOLDER_DESTINATION_RAW = 'propostas'
    file_name = 'base_jira_propostas.parquet'
    file_path = f'{FOLDER_DESTINATION_RAW}/{file_name}'


    # Lendo o arquivo da Raw
    response = client.get_object(BUCKET_SOURCE_RAW, file_path)
    file_data = BytesIO(response.read())
    df = pd.read_parquet(file_data)


    # Inspecionando colunas do DataFrame
    print("Colunas carregadas e disponíveis no DataFrame:")
    print(df.columns.tolist())


    print("Criando funções para tratamento de colunas no DataFrame...")
    def verifica_politica(politica_desc):
        if pd.isnull(politica_desc):
            return "Não atribuida"
        else:
            return politica_desc

    def verifica_parecer(parecer):
        if pd.isnull(parecer):
            return "Não atribuido"
        else:
            return parecer


    def verifica_ramificacao_motor(ramificacao):
        if pd.isnull(ramificacao):
            return "Não atribuida"
        else:
            return ramificacao

    def verifica_vendedor(vendedor):
        if pd.isnull(vendedor):
            return "Não atribuido"
        else:
            return vendedor
        
    def verifica_filial_fn(fn):
        if pd.isnull(fn):
            return "Não atribuído"
        else:
            return fn

    # Função para formatar o nome do decisor
    def formata_decisor(var_decisor):
        if pd.isnull(var_decisor):  # Verificando se é null (NaN)
            return "Não atribuido"
        if var_decisor == "Jira Service User":
            return "Motor"

        nome_parte = var_decisor.split('@')[0]
        partes = nome_parte.split('.')
        return ' '.join(part.title() for part in partes)

    def formata_status(status_func):
        status_dict = {
            'Analyzing Credit Score': "Analisando Pontuação de Crédito",
            'Attaching Documentation': "Anexando Documentação",
            'Awaiting Approval': "Aguardando Aprovação",
            'Awaiting BV docs': "Aguardando Documentos BV",
            'Awaiting Comitee Approval': "Aguardando Aprovação do Comitê",
            'Awaiting Comitee II Approval': "Aguardando Aprovação do Comitê II",
            'Awaiting Design': "Aguardando Design",
            'Awaiting Documentation': "Aguardando Documentação",
            'Awaiting Execution': "Aguardando Execução",
            'Awaiting Legal Approval': "Aguardando Aprovação Legal",
            'Awaiting Manager Approval': "Aguardando Aprovação do Gerente",
            'Awaiting Minor Approval': "Aguardando Menor Aprovação",
            'Awaiting Priorization CS': "Aguardando Priorização CS",
            'Awaiting Priorization Seller': "Aguardando Priorização Vendedor",
            'Error': "Erro",
            'Prioritized': "Priorizado",
            'Prioritizing CS': "Priorizando CS",
            'Prioritizing Seller': "Priorizando Vendedor",
            'Fechada': "Fechada",
            'Resolvido': "Resolvido",
            'Em Progresso': "Em Progresso",
            'Aberto': "Aberto"
    }

        # Verificando se alguma das chaves está contida em status_func
        for key in status_dict:
            if key in status_func:
                return status_dict[key]

        return "Outros"

    # Função para formatar decisão
    def formata_decisao(decisor_func):
        if decisor_func == 'Approved':
            return "Aprovado"
        elif decisor_func == 'Reproved':
            return "Reprovado"
        elif decisor_func == 'Duplicado':
            return "Duplicado"
        else:
            return "Não atribuido"
        

    # Função para classificar o tipo de proposta
    def classifica_tipo_proposta(proposta):
        if isinstance(proposta, str):  # Verifica se proposta é uma string
            if 'Solicitação de limite' in proposta or 'Análise de sacado' in proposta or 'Solicitação de Limite' in proposta or 'Solicitação de cadastro' in proposta:
                return "Solicitação de Limite"
            elif 'Transferencia de limite' in proposta or 'Transferência  de limite' in proposta or 'Transferência de limite' in proposta or 'Transferência de Limite' in proposta:
                return "Transferência de Limite"
            elif 'Solicitação de Over' in proposta or 'Solicitação de Overlimite' in proposta or 'Overlimit' in proposta:
                return "Solicitação de Overlimite"
            elif 'Zerar Limite' in proposta or 'Zerar limite' in proposta or 'Zerar Limte' in proposta or 'Zerar  limite' in proposta or 'Zerar  limite' in proposta or 'Zerar | Ajuste limite' in proposta:
                return "Zerar Limite"
            elif 'Ajuste de Limite' in proposta or 'Ajuste de limite' in proposta or 'Reajuste de limite' in proposta or 'Revisão de limite' in proposta or 'Remanejamento de limite' in proposta or 'Ajuste  de limite' in proposta or 'Ajuste  de limite' in proposta or 'Ajuste de LC' in proposta:
                return "Ajuste de Limite"
            elif 'Redução de limite' in proposta or 'Redução de Limite' in proposta or 'Reduzição de limite' in proposta or 'Resuzição de limite' in proposta or 'Limite' in proposta:
                return "Redução de Limite"
            elif 'Bloqueio Sacado ' in proposta:
                return "Bloqueio Sacado"
            elif 'LOTE' in proposta:
                return "Lote"
            elif 'CNPJ' in proposta or 'CNPJ Errado' in proposta:
                return "Verificar CNPJ"
            elif 'Majoração de limite' in proposta or 'Majoração de Limite' in proposta or 'Majoração  de limite' in proposta or 'MAjoração de limite' in proposta or 'Majoração limite' in proposta or 'Majoração' in proposta or 'Marojação de limite' in proposta or 'Majoraçãode limite' in proposta or 'Majoração limite' in proposta or 'Majoração limite/Transferência de LC' in proposta:
                return "Majoração de Limite"
            elif 'Baixa de Overlimit' in proposta or 'Baixa de Over' in proposta or 'Reduzir Over' in proposta or 'Overlimite' in proposta:
                return "Baixa de Overlimit"
            return "Outros"
        
    def formata_prioridade(nivel):
        if nivel == 'Low':
            return "Baixo"
        elif nivel == 'Medium':
            return "Médio"
        elif nivel == 'High':
            return "Alto"
        elif nivel == 'Lowest':
            return "Muito Baixo"
        elif nivel == 'Highest':
            return "Muito Alto"
        elif nivel == 'Unknown':
            return "Não atribuida"
        else:
            return "Não atribuida"
        
        
    print("Criação de funções finalizadas com sucesso!")


    print("Tratando colunas no DataFrame conforme as funções criadas...")

    # Aplicando funções para criar colunas com informações formatadas
    df['politica'] = df['politica'].apply(verifica_politica)
    df['nome_vendedor_fn'] = df['nome_vendedor_fn'].apply(verifica_vendedor)
    df['nome_vendedor_alpe'] = df['nome_vendedor_alpe'].apply(verifica_vendedor)
    df['filial_fn'] = df['filial_fn'].apply(verifica_filial_fn)
    df['tipo_proposta'] = df['nome_issue'].apply(classifica_tipo_proposta)
    df['decisor'] = df['decisor'].apply(formata_decisor)
    df['status'] = df['status'].apply(formata_status)
    df['ramificacao_motor'] = df['ramificacao_motor'].apply(verifica_ramificacao_motor)
    df['decisao'] = df['decisao'].apply(formata_decisao)
    df['parecer'] = df['parecer'].apply(verifica_parecer)
    df['prioridade'] = df['prioridade'].apply(formata_prioridade)


    # Convertendo colunas de data e hora para datetime
    df['data_criado'] = pd.to_datetime(df['data_criado'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    df['data_resolvido'] = pd.to_datetime(df['data_resolvido'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    df['data_atualizado'] = pd.to_datetime(df['data_atualizado'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    df['data_disponivel_mesa'] = pd.to_datetime(df['data_disponivel_mesa'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')


    # Adicionando colunas de data e hora
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df['year'], df['month'], df['day'] = now.year, now.month, now.day


    # Configurações para acesso ao MinIO
    logger = LoggingMixin().log 

    try:
        logger.info("Iniciando salvamento das informações")
            
        storage_options = {
            "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
            "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
            "AWS_ENDPOINT_URL":f"https://{access_params['endpoint_url_trusted']}",
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }


        # Definindo o caminho e salvando no MinIO
        BUCKET_SOURCE_TRUSTED = "jira"
        FOLDER_DESTINATION_TRUSTED = "propostas"

        write_deltalake(
            f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}", 
            df, 
            partition_by=["year", "month", "day"],
            storage_options=storage_options,
            mode="overwrite"
            #overwrite_schema=True
    )
        logger.info("Salvamento concluído com sucesso.")
        
    except Exception as e:
        logger.error(f"Erro ao salvar as informações: {str(e)}")