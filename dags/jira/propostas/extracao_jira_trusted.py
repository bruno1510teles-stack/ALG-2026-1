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

def base_details_trusted(access_params=None, **kwargs):
    # Conectando na Raw para Leitura
    client = Minio(
        access_params['endpoint_url_raw'],
        access_key=access_params['aws_access_key_id_raw'],
        secret_key=access_params['aws_secret_access_key_raw']
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
            return "Politica não atribuída"
        else:
            return politica_desc
    
    def verifica_parecer(parecer):
        if pd.isnull(parecer):
            return "Descrição Parecer não atribuída"
        else:
            return parecer


    def verifica_ramificacao_motor(ramificacao):
        if pd.isnull(ramificacao):
            return "Ramificação não atribuída"
        else:
            return ramificacao

    def verifica_vendedor(vendedor):
        if pd.isnull(vendedor):
            return "Vendedor não atribuído"
        else:
            return vendedor
        
    def verifica_filial_fn(fn):
        if pd.isnull(fn):
            return "Filial Fornecedor não atribuída"
        else:
            return fn


    # Função para formatar o nome do decisor
    def formata_decisor(var_decisor):
        if pd.isnull(var_decisor):  # Verificando se é null (NaN)
            return "Decisor não atribuído"
        if var_decisor == "Jira Service User":
            return "Motor"

        nome_parte = var_decisor.split('@')[0]
        partes = nome_parte.split('.')
        return ' '.join(part.title() for part in partes)

    def formata_status(status_func):
        status_dict = {
            'Analyzing Credit Score': "Análise da Pontuação de Crédito",
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
            return "Decisão não atribuida"
        


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
            return "Prioridade não definida"
        else:
            return "Prioridade não atribuida"
        
        
    print("Criação de funções finalizadas com sucesso!")
    
    
    
    print("Tratando colunas no DataFrame conforme as funções criadas...")
    # Adicionando colunas de data e hora
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df['year'], df['month'], df['day'] = now.year, now.month, now.day

    # Aplicando funções para criar colunas com informações formatadas
    df['politica_desc'] = df['politica'].apply(verifica_politica)
    df['vendedor_fornecedor'] = df['nome_vendedor_fn'].apply(verifica_vendedor)
    df['vendedor_alpe'] = df['nome_vendedor_alpe'].apply(verifica_vendedor)
    df['filial_fornecedor'] = df['filial_fn'].apply(verifica_filial_fn)
    df['tipo_proposta'] = df['nome_issue'].apply(classifica_tipo_proposta)
    df['nome_decisor'] = df['decisor'].apply(formata_decisor)
    df['tipo_status'] = df['status'].apply(formata_status)
    df['ramificacao_motor_desc'] = df['ramificacao_motor'].apply(verifica_ramificacao_motor)
    df['status_decisao'] = df['decisao'].apply(formata_decisao)
    df['parecer_desc'] = df['parecer'].apply(verifica_parecer)
    df['prioridade_desc'] = df['prioridade'].apply(formata_prioridade)


    # Convertendo colunas de data e hora
    df['criado_tratado'] = pd.to_datetime(df['criado'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
    df['resolvido_tratado'] = pd.to_datetime(df['resolvido'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
    df['aguardando_aprovacao_gerente_tratado'] = pd.to_datetime(df['AWAITING MANAGER APPROVAL'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
    df['analise_pontuacao_credito_tratado'] = pd.to_datetime(df['Analyzing Credit Score'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
    df['anexando_documentacao_tratado'] = pd.to_datetime(df['Attaching Documentation'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
    df['aguardando_aprovacao_tratado'] = pd.to_datetime(df['Awaiting Approval'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
    df['aguardando_documentos_bv_tratado'] = pd.to_datetime(df['Awaiting BV docs'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
    df['aguardando_aprovacao_comite_tratado'] = pd.to_datetime(df['Awaiting Comitee Approval'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
    df['aguardando_aprovacao_comite_2_tratado'] = pd.to_datetime(df['Awaiting Comitee II Approval'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
    df['aguardando_documentacao_tratado'] = pd.to_datetime(df['Awaiting Documentation'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
    df['aguardando_execucao_tratado'] = pd.to_datetime(df['Awaiting Execution'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
    df['aguardando_aprovacao_legal_tratado'] = pd.to_datetime(df['Awaiting Legal Approval'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
    df['aguardando_menor_aprovacao_tratado'] = pd.to_datetime(df['Awaiting Minor Approval'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
    df['aguardando_priorizacao_cs_tratado'] = pd.to_datetime(df['Awaiting Priorization CS'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
    df['priorizacao_cs_tratado'] = pd.to_datetime(df['PRIORITIZING CS'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
    df['aguardando_priorizacao_vendedor_tratado'] = pd.to_datetime(df['Awaiting Priorization Seller'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
    df['fechado_tratado'] = pd.to_datetime(df['Closed'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
    df['erro_tratado'] = pd.to_datetime(df['Error'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
    df['em_progresso_tratado'] = pd.to_datetime(df['In Progress'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
    df['reprovacao_massa_tratado'] = pd.to_datetime(df['Mass Reproval'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
    df['aberto_tratado'] = pd.to_datetime(df['Open'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')
    df['resolvido_status_tratado'] = pd.to_datetime(df['Resolved'], format="%Y-%m-%dT%H:%M:%S.%f%z", errors='coerce')

    
    # Convertendo colunas de data e hora Status SLA
    
    df['data_criado'] = df['criado_tratado'].dt.date
    df['hora_criado'] = df['criado_tratado'].dt.strftime('%H:%M:%S')
    
    df['data_resolvido'] = df['resolvido_tratado'].dt.date
    df['hora_resolvido'] = df['resolvido_tratado'].dt.strftime('%H:%M:%S')
    
    df['data_aguardando_aprovacao_gerente'] = df['AWAITING MANAGER APPROVAL'].dt.date
    df['time_aguardando_aprovacao_gerente'] = df['AWAITING MANAGER APPROVAL'].dt.strftime('%H:%M:%S')
    
    df['data_analise_pontuacao_credito'] = df['Analyzing Credit Score'].dt.date
    df['time_analise_pontuacao_credito'] = df['Analyzing Credit Score'].dt.strftime('%H:%M:%S')
    
    df['data_anexando_documentacao'] = df['Attaching Documentation'].dt.date
    df['time_anexando_documentacao'] = df['Attaching Documentation'].dt.strftime('%H:%M:%S')
    
    df['data_aguardando_aprovacao'] = df['Awaiting Approval'].dt.date
    df['time_aguardando_aprovacao'] = df['Awaiting Approval'].dt.strftime('%H:%M:%S')
    
    df['data_aguardando_documentos_bv'] = df['Awaiting BV docs'].dt.date
    df['time_aguardando_documentos_bv'] = df['Awaiting BV docs'].dt.strftime('%H:%M:%S')
    
    df['data_aguardando_aprovacao_comite'] = df['Awaiting Comitee Approval'].dt.date
    df['time_aguardando_aprovacao_comite'] = df['Awaiting Comitee Approval'].dt.strftime('%H:%M:%S')
    
    df['data_aguardando_aprovacao_comite_2'] = df['Awaiting Comitee II Approval'].dt.date
    df['time_aguardando_aprovacao_comite_2'] = df['Awaiting Comitee II Approval'].dt.strftime('%H:%M:%S')
    
    df['data_aguardando_documentacao'] = df['Awaiting Documentation'].dt.date
    df['time_aguardando_documentacao'] = df['Awaiting Documentation'].dt.strftime('%H:%M:%S')
    
    df['data_aguardando_execucao'] = df['Awaiting Execution'].dt.date
    df['time_aguardando_execucao'] = df['Awaiting Execution'].dt.strftime('%H:%M:%S')
    
    df['data_aguardando_aprovacao_legal'] = df['Awaiting Legal Approval'].dt.date
    df['time_aguardando_aprovacao_legal'] = df['Awaiting Legal Approval'].dt.strftime('%H:%M:%S')
    
    df['data_aguardando_menor_aprovacao'] = df['Awaiting Minor Approval'].dt.date
    df['time_aguardando_menor_aprovacao'] = df['Awaiting Minor Approval'].dt.strftime('%H:%M:%S')
    
    df['data_aguardando_priorizacao_cs'] = df['Awaiting Priorization CS'].dt.date
    df['time_aguardando_priorizacao_cs'] = df['Awaiting Priorization CS'].dt.strftime('%H:%M:%S')
    
    df['data_priorizacao_cs'] = df['PRIORITIZING CS'].dt.date
    df['time_priorizacao_cs'] = df['PRIORITIZING CS'].dt.strftime('%H:%M:%S')
    
    df['data_aguardando_priorizacao_vendedor'] = df['Awaiting Priorization Seller'].dt.date
    df['time_aguardando_priorizacao_vendedor'] = df['Awaiting Priorization Seller'].dt.strftime('%H:%M:%S')
    
    df['data_fechado'] = df['Closed'].dt.date
    df['time_fechado'] = df['Closed'].dt.strftime('%H:%M:%S')
    
    df['data_erro'] = df['Error'].dt.date
    df['time_erro'] = df['Error'].dt.strftime('%H:%M:%S')
    
    df['data_em_progresso'] = df['In Progress'].dt.date
    df['time_em_progresso'] = df['In Progress'].dt.strftime('%H:%M:%S')
    
    df['data_reprovação_massa'] = df['Mass Reproval'].dt.date
    df['time_reprovação_massa'] = df['Mass Reproval'].dt.strftime('%H:%M:%S')
    
    df['data_aberto'] = df['Open'].dt.date
    df['time_aberto'] = df['Open'].dt.strftime('%H:%M:%S')
    
    df['data_resolvido_status'] = df['Resolved'].dt.date
    df['time_resolvido_status'] = df['Resolved'].dt.strftime('%H:%M:%S')

    # Aplicando o filtro para criar o DataFrame final
    filtro = df[
        (~df['decisao'].isin(["Não será feito", "Canceled", "Duplicado"])) &
        (~df['status'].isin(["Error"])) &
        (df['limite_pedido'] >= 0) & 
        (df['limite_pedido'] <= 500000000)
    ]

    # Selecionando as colunas relevantes
    jira_tratado = filtro[[
        'issue_key', 'politica_desc', 'cnpj', 'pgid', 'limite_pedido', 'limite_aprovado', 'nome_issue',
        'tipo_proposta', 'vendedor_alpe', 'vendedor_fornecedor', 'filial_fornecedor',
        'tipo_status','nome_decisor', 'status_decisao','parecer_desc', 'ramificacao_motor_desc',
        'resolvido_tratado', 'criado_tratado',
        'analise_pontuacao_credito_tratado','anexando_documentacao_tratado','aguardando_aprovacao_tratado',
        'aguardando_documentos_bv_tratado','aguardando_aprovacao_comite_tratado','aguardando_aprovacao_comite_2_tratado',
        'aguardando_documentacao_tratado','aguardando_execucao_tratado', 'aguardando_aprovacao_legal_tratado',
        'aguardando_menor_aprovacao_tratado','aguardando_priorizacao_cs_tratado','priorizacao_cs_tratado',
        'aguardando_priorizacao_vendedor_tratado','fechado_tratado','erro_tratado','em_progresso_tratado',
        'reprovacao_massa_tratado','aberto_tratado','resolvido_status_tratado',
        'data_criado', 'hora_criado',
        'data_resolvido', 'hora_resolvido',
        'data_aguardando_aprovacao_gerente', 'time_aguardando_aprovacao_gerente',
        'data_analise_pontuacao_credito', 'time_analise_pontuacao_credito',
        'data_anexando_documentacao', 'time_anexando_documentacao',
        'data_aguardando_aprovacao', 'time_aguardando_aprovacao',
        'data_aguardando_documentos_bv', 'time_aguardando_documentos_bv',
        'data_aguardando_aprovacao_comite', 'time_aguardando_aprovacao_comite',
        'data_aguardando_aprovacao_comite_2', 'time_aguardando_aprovacao_comite_2',
        'data_aguardando_documentacao', 'time_aguardando_documentacao',
        'data_aguardando_execucao', 'time_aguardando_execucao',
        'data_aguardando_aprovacao_legal', 'time_aguardando_aprovacao_legal',
        'data_aguardando_menor_aprovacao', 'time_aguardando_menor_aprovacao',
        'data_aguardando_priorizacao_cs', 'time_aguardando_priorizacao_cs',
        'data_priorizacao_cs', 'time_priorizacao_cs',
        'data_aguardando_priorizacao_vendedor', 'time_aguardando_priorizacao_vendedor',
        'data_fechado', 'time_fechado',
        'data_erro', 'time_erro',
        'data_em_progresso', 'time_em_progresso',
        'data_reprovação_massa', 'time_reprovação_massa',
        'data_aberto', 'time_aberto',
        'data_resolvido_status', 'time_resolvido_status',
        'atualizado_em', 'year', 'month', 'day'
    ]].reset_index(drop=True)

    # Exibindo o DataFrame tratado
    print(f"{len(jira_tratado)} propostas válidas.")
    print(jira_tratado)



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
            jira_tratado, 
            partition_by=["year", "month", "day"],
            storage_options=storage_options,
            mode="overwrite"
            #overwrite_schema=True
    )
        logger.info("Salvamento concluído com sucesso.")
        
    except Exception as e:
        logger.error(f"Erro ao salvar as informações: {str(e)}")