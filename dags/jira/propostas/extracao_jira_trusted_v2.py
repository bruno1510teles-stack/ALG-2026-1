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


    # Filtrando apenas casos Fechados ou Resolvidos

    df_resolvido = df[(df['status'] == 'Fechada') | (df['status'] == 'Resolvido')].copy()

    print(f"Quantidade de linhas com status \"Resolvido\" ou \"Fechada\": {df_resolvido.shape[0]}")


    # TRATANDO COLUNA DA POLITICA
    def verifica_politica(politica_desc):
        if pd.isnull(politica_desc):
            return "NÃO ATRIBUIDA"
        else:
            return politica_desc

    df_resolvido['politica'] = df_resolvido['politica'].apply(verifica_politica).str.upper()


    # CRIANDO RAIZ CNPJ 8
    df_resolvido['raiz_cnpj'] = df_resolvido['cnpj'].astype(str).str[:8]


    # TRATANDO PGID
    def verifica_pgid(pgid):
        if pd.isnull(pgid):
            return "NÃO ATRIBUIDA"
        else:
            return pgid

    df_resolvido['pgid'] = df_resolvido['pgid'].apply(verifica_pgid).str.upper()


    # Substituindo valores nulos por 0 nas colunas 'limite_pedido' e 'limite_aprovado'
    df_resolvido['limite_pedido'] = df_resolvido['limite_pedido'].fillna(0)
    df_resolvido['limite_aprovado'] = df_resolvido['limite_aprovado'].fillna(0)


    # Nome_Issue Maiusculo
    df_resolvido['nome_issue'] = df_resolvido['nome_issue'].str.upper()


    # TRATANDO VENDEDOR ALPE
    def verifica_vendedor(vendedor):
        if pd.isnull(vendedor):
            return "NÃO ATRIBUIDA"
        else:
            return vendedor

    df_resolvido['nome_vendedor_alpe'] = df_resolvido['nome_vendedor_alpe'].apply(verifica_vendedor).str.upper().str.strip()


    # PADRONIZANDO OS NOMES
    nome_padronizado = {
        'NÃO ATRIBUIDA': 'NÃO ATRIBUIDA',
        'ALEXANDRE SANTOS CARMO': 'ALEXANDRE SANTOS CARMO',
        'ALPE QUE REALIZOU A SOLICITAÇÃO': 'ALPE',
        'CAMILA CABRAL': 'CAMILA CABRAL',
        'CAROLINE FREIHAT': 'CAROLINE FREIHAT',
        ('CASSIO ESTEVES', 'CASSIO FILIPE ALVES ESTEVES') : 'CASSIO ESTEVES',
        ('CLAUDIA CINARE', 'CLAUDIA ETO', 'CLAUDIA RODIGUES', 'CLAUDIA RODRIGUES') : 'CLAUDIA CINARE',
        ('DIANA TIEMI', 'DIANA TIENI', 'DIANA YAMAMOTO') : 'DIANA TIEMI',
        'EDER CAVALCANTE': 'EDER CAVALCANTE',
        'ELAINE FABIANA BARBOSA': 'ELAINE FABIANA BARBOSA',
        'EMANUELLE CATORI': 'EMANUELLE CATORI',
        'FATURAMENTO': 'FATURAMENTO',
        ('GLAUCIANE OLIVEIRA', 'GLAUCIELE OLIVEIRA') : 'GLAUCIELE OLIVEIRA',
        ('JOSE CARVALHO', 'JOSÉ CARVALHO', 'JOSE VICTOR', 'JOSE VITOR'): 'JOSE VITOR',
        ('LARISSA - CRÉDITO', 'LARISSA FREIRE', 'LARISSA SOARES', 'LARISSA SOARES - CRÉDITO') : 'LARISSA FREIRE',
        ('LEANDRO ANUNCIAÇÃO', 'LEANDRO QUINTINO') : 'LEANDRO QUINTINO',
        'LEDIMIR HENRIQUE': 'LEDIMIR HENRIQUE',
        ('MAYCON HELDER', 'MAYCON HELDER]', 'MAYCON OLIVEIRA'): 'MAYCON HELDER',   
        'MILEIDE VIEIRA': 'MILEIDE VIEIRA',
        'NILTON SANTOS': 'NILTON SANTOS',
        ('PEDRO VINICIUS', 'PEDRO ALVES'): 'PEDRO ALVES',
        'PORTAL ARCELOR': 'PORTAL ARCELOR',
        'PRISCILA YURI': 'PRISCILA YURI',
        ('PRISCILLA MORAES', 'PRISCILLA COSTA'): 'PRISCILLA COSTA',  
        'RAFAEL CANTAGALLI': 'RAFAEL CANTAGALLI',
        'RAFAELA - ASUS': 'RAFAELA - ASUS',
        ('ROGERIO', 'ROGERIO FRIAS'): 'ROGERIO FRIAS',
        ('ROSEMEIRE DIAS', 'ROSEMEIRE FERREIRA') : 'ROSEMEIRE DIAS',
        ('TALITA LIANDRA DA SILVA RODRIGUES', 'TALITA RODRIGUES') : 'TALITA LIANDRA DA SILVA RODRIGUES',
        'THAIS DAS NEVES' : 'THAIS DAS NEVES',
        ('TIAGO CARVALHO', 'TIAGO.CARVALHO@ALPE.COM.BR') : 'TIAGO CARVALHO',
        'VANESSA LINO': 'VANESSA LINO',
        ('WILMA CARLA ROCHA SANTOS', 'WILMA SANTOS'): 'WILMA SANTOS',
        'JOEL DONIZETTI APARECIDO': 'JOEL DONIZETTI APARECIDO'
    }

    # Função para padronizar o nome
    def padronizar_nome_vendedor_alpe(nome):
        # Percorre o dicionário e verifica se o nome está na chave
        for key, value in nome_padronizado.items():
            if isinstance(key, tuple):  # Se a chave for uma tupla (vários nomes)
                if nome in key:
                    return value
            else:
                if nome == key:
                    return value
        # Se o nome não estiver no dicionário, retorna uma mensagem para adicionar
        return 'ADICIONAR NO DICIONARIO DE NOMES'

    df_resolvido['nome_vendedor_alpe_tratado'] = df_resolvido['nome_vendedor_alpe'].apply(padronizar_nome_vendedor_alpe)

    # DEFININDO OS NOMES DOS VENDEDORES ALPE, TUDO QUE NAO ESTIVER NA LISTA, SETAR "OUTROS"

    vendedores_alpe = [
    'CAROLINE FREIHAT', 'CLAUDIA CINARE', 'DIANA TIEMI', 'JOSE VITOR', 'LARISSA FREIRE',
    'LEANDRO QUINTINO', 'ROGERIO FRIAS', 'ROSEMEIRE DIAS', 'VANESSA LINO', 'CAMILA CABRAL'
    ]

    df_resolvido['nome_vendedor_alpe_tratado'] = df_resolvido['nome_vendedor_alpe_tratado'].apply(
        lambda x: x if x in vendedores_alpe else 'OUTROS'
    )

    # TRATANDO VENDEDOR FN
    def verifica_vendedor_fn(fn):
        if pd.isnull(fn):
            return "NÃO ATRIBUIDA"
        else:
            return fn

    df_resolvido['nome_vendedor_fn'] = df_resolvido['nome_vendedor_fn'].apply(verifica_vendedor_fn).str.upper()


    # TRATANDO FILIAL FN
    def verifica_filial_fn(fn):
        if pd.isnull(fn):
            return "NÃO ATRIBUIDA"
        else:
            return fn

    df_resolvido['filial_fn'] = df_resolvido['filial_fn'].apply(verifica_filial_fn).str.upper()


    # TRATANDO PRIORIDADE
    def formata_prioridade(nivel):
        if nivel == 'Low':
            return "BAIXO"
        elif nivel == 'Medium':
            return "MEDIO"
        elif nivel == 'High':
            return "ALTO"
        elif nivel == 'Lowest':
            return "MUITO BAIXO"
        elif nivel == 'Highest':
            return "MUITO ALTO"
        elif nivel == 'Unknown':
            return "NÃO ATRIBUIDA"
        else:
            return "NÃO ATRIBUIDA"

    df_resolvido['prioridade'] = df_resolvido['prioridade'].apply(formata_prioridade).str.upper()


    # TRATANDO STATUS
    df_resolvido['status'] = 'RESOLVIDO'


    # TRATANDO DECISOR
    def verifica_decisor(decisor):
        if pd.isnull(decisor):
            return "NÃO ATRIBUIDA"
        else:
            return decisor

    df_resolvido['decisor'] = df_resolvido['decisor'].apply(verifica_decisor).str.upper()

    # Lista de nomes por categorias
    motor = [
        'MOTOR', 'JIRA SERVICE USER'
    ]

    mesa = [
        'DIANA TIEMI YAMAMOTO', 'VANESSA SOUZA', 'LARISSA FREIRE SOARES', 'ROSEMEIRE DIAS FERREIRA', 
        'JOSE CARVALHO', 'CAROLINE FREIHAT HENRIQUE DE ALCANTARA SANTANA', 'LEANDRO QUINTINO DA ANUNCIACAO', 
        'CLAUDIA CINARE RODRIGUES ETO', 'ROGERIO DE CAMPOS FRIAS', 
        'CAMILA MAMEDE CABRAL', 'BEATRIZ PEREIRA GAMA CARDOSO', 'ANA BEATRIZ RODRIGUES ANDRADE', 
        'VINÍCIUS GABRIEL FERREIRA RIBEIRO', 'VITÓRIA SILVA DOS REIS', 'THIAGO ASSIS', 'JOSE.CARVALHO@ALPE.COM.BR'
    ]

    outros = [
        'NÃO ATRIBUIDA', 'CLAUDIA CRAVO', 'RAFAEL ROCHA LEITE', 'VIVIAN POMPEU', 'MAYARA COSTA', 
        'PRISCILA YURI NAGATA ORTEGA', 'MAYARA.COSTA' , 'AUGUSTO DE ABREU'
    ]

    # Função para atribuir categorias
    def categorizar_decisor(nome):
        nome = nome.upper()
        if nome in motor:
            return 'MOTOR'
        elif nome in mesa:
            return 'MESA'
        elif nome in outros:
            return 'OUTROS'
        else:
            return 'ADICIONAR NO DICIONARIO DE NOMES'


    # Aplicando a função de categorizar no DataFrame
    df_resolvido['categoria_decisor'] = df_resolvido['decisor'].apply(categorizar_decisor)


    # TRATANDO DECISAO
    def formata_decisao(decisor_func):
        if decisor_func == 'Approved':
            return "APROVADO"
        elif decisor_func == 'Reproved':
            return "REPROVADO"
        elif decisor_func == 'Duplicado':
            return "DUPLICADO"
        elif decisor_func == 'Ineligible':
            return "REPROVADO"
        elif decisor_func == 'Canceled':
            return "CANCELADO"
        elif decisor_func == 'Não será feito':
            return "REPROVADO"
        elif decisor_func == 'Concluído':
            return "APROVADO"
        else:
            return "NÃO ATRIBUIDA"

    df_resolvido['decisao'] = df_resolvido['decisao'].apply(formata_decisao).str.upper()



    #TRATANDO RAMIFICACAO MOTOR
    def verifica_ramificacao(ramificacao):
        if pd.isnull(ramificacao):
            return "NÃO ATRIBUIDA"
        else:
            return ramificacao

    df_resolvido['ramificacao_motor'] = df_resolvido['ramificacao_motor'].apply(verifica_ramificacao).str.upper()


    def classifica_ramificacao(ramificacao):
        if ramificacao == 'PF 1':
            return 'PF CNPJ IRREGULAR'
        elif ramificacao == 'PF 11':
            return 'PF JA TEVE ANALISE ANTERIOR ALPE'
        elif ramificacao == 'PF 2':
            return 'PF RJ'
        elif ramificacao == 'PF 3':
            return 'PF MEI'
        elif ramificacao == 'PF 10':
            return 'PF INAD ALPE'
        elif ramificacao == 'PF 4':
            return 'PF CNAE'
        elif ramificacao == 'PF 5':
            return 'PF CONSORCIO/CONSTRUTORA/SPE'
        elif ramificacao == 'PF 6':
            return 'PF NATUREZA JURIDICA'
        elif ramificacao == 'PF 7':
            return 'PF PEP'
        elif ramificacao == 'PF 8':
            return 'PF SOCIO < 2 ANOS'
        elif ramificacao == 'PF 9':
            return 'PF FUNDACAO < 2 ANOS'
        else:
            return ramificacao  # Caso não corresponda a nenhum valor, retorna o próprio valor

    # Exemplo de uso no DataFrame
    df_resolvido['ramificacao_motor'] = df_resolvido['ramificacao_motor'].apply(classifica_ramificacao).str.upper()


    # CRIANDO COLUNA TIPO DA PROPOSTA
    def classifica_tipo_proposta(proposta):
        if isinstance(proposta, str):  # Verifica se proposta é uma string
            proposta = proposta.upper()  # Converte para maiúsculas
            
            if 'SOLICITAÇÃO DE LIMITE' in proposta or 'ANÁLISE DE SACADO' in proposta or 'SOLICITAÇÃO DE LIMITE' in proposta or 'SOLICITAÇÃO DE CADASTRO' in proposta:
                return "SOLICITAÇÃO DE LIMITE"
            elif 'TRANSFERENCIA DE LIMITE' in proposta or 'TRANSFERÊNCIA  DE LIMITE' in proposta or 'TRANSFERÊNCIA DE LIMITE' in proposta or 'TRANSFERÊNCIA DE LIMITE' in proposta:
                return "TRANSFERÊNCIA DE LIMITE"
            elif 'SOLICITAÇÃO DE OVER' in proposta or 'SOLICITAÇÃO DE OVERLIMITE' in proposta or 'OVERLIMIT' in proposta:
                return "SOLICITAÇÃO DE OVERLIMIT"
            elif 'ZERAR LIMITE' in proposta or 'ZERAR LIMITE' in proposta or 'ZERAR LIMTE' in proposta or 'ZERAR  LIMITE' in proposta or 'ZERAR  LIMITE' in proposta or 'ZERAR | AJUSTE LIMITE' in proposta:
                return "ZERAR LIMITE"
            elif 'AJUSTE DE LIMITE' in proposta or 'AJUSTE DE LIMITE' in proposta or 'REAJUSTE DE LIMITE' in proposta or 'REVISÃO DE LIMITE' in proposta or 'REMANEJAMENTO DE LIMITE' in proposta or 'AJUSTE  DE LIMITE' in proposta or 'AJUSTE  DE LIMITE' in proposta or 'AJUSTE DE LC' in proposta:
                return "AJUSTE DE LIMITE"
            elif 'REDUÇÃO DE LIMITE' in proposta or 'REDUÇÃO DE LIMITE' in proposta or 'REDUÇÃO DE LIMITE' in proposta or 'REDUÇÃO DE LIMITE' in proposta:
                return "REDUÇÃO DE LIMITE"
            elif 'BLOQUEIO SACADO' in proposta:
                return "BLOQUEIO SACADO"
            elif 'LOTE' in proposta:
                return "LOTE"
            elif 'CNPJ' in proposta or 'CNPJ ERRADO' in proposta:
                return "VERIFICAR CNPJ"
            elif 'MAJORAÇÃO DE LIMITE' in proposta or 'MAJORAÇÃO DE LIMITE' in proposta or 'MAJORAÇÃO  DE LIMITE' in proposta or 'MAJORAÇÃO DE LIMITE' in proposta or 'MAJORAÇÃO LIMITE' in proposta or 'MAJORAÇÃO' in proposta or 'MAJORAÇÃO DE LIMITE' in proposta or 'MAJORAÇÃO DE LIMITE/TRANSFERÊNCIA DE LC' in proposta:
                return "MAJORAÇÃO DE LIMITE"
            elif 'BAIXA DE OVERLIMIT' in proposta or 'BAIXA DE OVER' in proposta or 'REDUZIR OVER' in proposta or 'OVERLIMITE' in proposta:
                return "BAIXA DE OVERLIMIT"
            
            return "OUTROS"

    df_resolvido['tipo_proposta'] = df_resolvido['nome_issue'].apply(classifica_tipo_proposta)


    # CONVERTENDO COLUNAS DE DATA
    df_resolvido['data_criado'] = pd.to_datetime(df_resolvido['data_criado'], errors='coerce')
    df_resolvido['data_resolvido'] = pd.to_datetime(df_resolvido['data_resolvido'], errors='coerce')
    df_resolvido['data_atualizado'] = pd.to_datetime(df_resolvido['data_atualizado'], errors='coerce')
    df_resolvido['data_disponivel_mesa'] = pd.to_datetime(df_resolvido['data_disponivel_mesa'], errors='coerce')


    # Adicionando colunas de data e hora
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_resolvido['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_resolvido['year'], df_resolvido['month'], df_resolvido['day'] = now.year, now.month, now.day


    # SELECIONA AS COLUNAS PARA EXPORTAR
    df_final = df_resolvido[[
        'issue_key', 'politica', 'cnpj', 'raiz_cnpj', 'pgid', 'limite_pedido',
        'limite_aprovado', 'nome_issue', 'nome_vendedor_alpe', 'nome_vendedor_alpe_tratado',
        'nome_vendedor_fn', 'filial_fn', 'prioridade', 'status', 'decisor',
        'categoria_decisor',
        'decisao', 'parecer', 'ramificacao_motor', 'tipo_proposta', 'data_criado',
        'data_resolvido', 'data_atualizado', 'data_disponivel_mesa',
        'atualizado_em', 'year', 'month', 'day'
        ]
    ].reset_index(drop=True)\

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
            df_final, 
            partition_by=["year", "month", "day"],
            storage_options=storage_options,
            mode="overwrite"
            #overwrite_schema=True
    )
        logger.info("Salvamento concluído com sucesso.")
        
    except Exception as e:
        logger.error(f"Erro ao salvar as informações: {str(e)}")