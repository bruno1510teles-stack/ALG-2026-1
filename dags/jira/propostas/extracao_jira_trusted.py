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
        'ALEXANDRE SANTOS CARMO': 'OUTROS',
        'ALPE QUE REALIZOU A SOLICITAÇÃO': 'OUTROS',
        'CAMILA CABRAL': 'OUTROS',
        'CAROLINE FREIHAT': 'OUTROS',
        ('CASSIO ESTEVES', 'CASSIO FILIPE ALVES ESTEVES') : 'CASSIO ESTEVES',
        ('CLAUDIA CINARE', 'CLAUDIA ETO', 'CLAUDIA RODIGUES', 'CLAUDIA RODRIGUES') : 'OUTROS',
        ('DIANA TIEMI', 'DIANA TIENI', 'DIANA YAMAMOTO') : 'OUTROS',
        'EDER CAVALCANTE': 'OUTROS',
        'ELAINE FABIANA BARBOSA': 'OUTROS',
        'EMANUELLE CATORI': 'OUTROS',
        'FATURAMENTO': 'OUTROS',
        ('GLAUCIANE OLIVEIRA', 'GLAUCIELE OLIVEIRA') : 'GLAUCIELE OLIVEIRA',
        ('JOSE CARVALHO', 'JOSÉ CARVALHO', 'JOSE VICTOR', 'JOSE VITOR'): 'OUTROS',
        ('LARISSA - CRÉDITO', 'LARISSA FREIRE', 'LARISSA SOARES', 'LARISSA SOARES - CRÉDITO') : 'OUTROS',
        ('LEANDRO ANUNCIAÇÃO', 'LEANDRO QUINTINO') : 'OUTROS',
        'LEDIMIR HENRIQUE': 'OUTROS',
        ('MAYCON HELDER', 'MAYCON HELDER]', 'MAYCON OLIVEIRA'): 'MAYCON HELDER',   
        'MILEIDE VIEIRA': 'OUTROS',
        'NILTON SANTOS': 'OUTROS',
        ('PEDRO VINICIUS', 'PEDRO ALVES'): 'PEDRO VINICIUS',
        'PORTAL ARCELOR': 'OUTROS',
        'PRISCILA YURI': 'OUTROS',
        ('PRISCILLA MORAES', 'PRISCILLA COSTA'): 'PRISCILLA COSTA',  
        'RAFAEL CANTAGALLI': 'RAFAEL CANTAGALLI',
        'RAFAELA - ASUS': 'OUTROS',
        ('ROGERIO', 'ROGERIO FRIAS'): 'OUTROS',
        ('ROSEMEIRE DIAS', 'ROSEMEIRE FERREIRA') : 'OUTROS',
        ('TALITA LIANDRA DA SILVA RODRIGUES', 'TALITA RODRIGUES') : 'TALITA LIANDRA',
        'THAIS DAS NEVES' : 'OUTROS',
        ('TIAGO CARVALHO', 'TIAGO.CARVALHO@ALPE.COM.BR') : 'TIAGO CARVALHO',
        'VANESSA LINO': 'OUTROS',
        ('WILMA CARLA ROCHA SANTOS', 'WILMA SANTOS'): 'WILMA SANTOS',
        'JOEL DONIZETTI APARECIDO': 'OUTROS',
        'COMERCIAL CONEXÃO' : 'OUTROS'
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
        return 'ADICIONAR NO DICIONARIO DE NOMES DE GERENTES'

    df_resolvido['gerente_tratado'] = df_resolvido['nome_vendedor_alpe'].apply(padronizar_nome_vendedor_alpe)


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

    # TRATA VALORES ESPECIFICOS DO DECISOR
    def verifica_decisor_v2(valor):
        if valor == "JOSE.CARVALHO@ALPE.COM.BR":
            return "JOSE CARVALHO"
        elif valor == "JIRA SERVICE USER":
            return "MOTOR"
        else:
            return valor

    df_resolvido['decisor'] = df_resolvido['decisor'].apply(verifica_decisor_v2).str.upper()

    # Lista de nomes por categorias
    motor = [
        'MOTOR', 'JIRA SERVICE USER'
    ]

    mesa = [
        'DIANA TIEMI YAMAMOTO', 'VANESSA SOUZA', 'LARISSA FREIRE SOARES', 'ROSEMEIRE DIAS FERREIRA', 
        'JOSE CARVALHO', 'CAROLINE FREIHAT HENRIQUE DE ALCANTARA SANTANA', 'LEANDRO QUINTINO DA ANUNCIACAO', 
        'CLAUDIA CINARE RODRIGUES ETO', 'ROGERIO DE CAMPOS FRIAS', 
        'ANA BEATRIZ RODRIGUES ANDRADE'      
    ]

    outros = [
        'NÃO ATRIBUIDA', 'CLAUDIA CRAVO', 'RAFAEL ROCHA LEITE', 'VIVIAN POMPEU', 'MAYARA COSTA', 
        'PRISCILA YURI NAGATA ORTEGA', 'MAYARA.COSTA' , 'AUGUSTO DE ABREU', 'CAMILA MAMEDE CABRAL', 'BEATRIZ PEREIRA GAMA CARDOSO',
        'VINÍCIUS GABRIEL FERREIRA RIBEIRO', 'VITÓRIA SILVA DOS REIS', 'THIAGO ASSIS'
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
            return 'ADICIONAR NO DICIONARIO DE NOMES DE ANALISTAS'


    # Aplicando a função de categorizar no DataFrame
    df_resolvido['categoria_decisor'] = df_resolvido['decisor'].apply(categorizar_decisor)



    # Função para atribuir categorias
    def categorizar_decisor_analista(nome):
        nome = nome.upper()
        if nome in motor:
            return 'MOTOR'
        elif nome in mesa:
            return nome  # Retorna o próprio nome, se estiver na lista de 'mesa'
        elif nome in outros:
            return 'OUTROS'
        else:
            return 'ADICIONAR NO DICIONARIO DE NOMES ANALISTAS'
        
    # Aplicando a função de categorizar no DataFrame
    df_resolvido['analista_tratado'] = df_resolvido['decisor'].apply(categorizar_decisor_analista)


    # TRATANDO DECISAO
    def formata_decisao(decisor_func):
        if decisor_func == 'Approved':
            return "APROVADO"
        elif decisor_func == 'Reproved':
            return "REPROVADO"
        elif decisor_func == 'Duplicado':
            return "DUPLICADO"
        elif decisor_func == 'Ineligible':
            return "INELEGÍVEL"
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


    # LISTA DE NOMES POR CARGO
    lista_cargo_motor = ['MOTOR']
    lista_cargo_assistente = ['ANA BEATRIZ RODRIGUES ANDRADE']
    lista_cargo_junior = ['LARISSA FREIRE SOARES', 'VANESSA SOUZA']
    lista_cargo_pleno = ['LEANDRO QUINTINO DA ANUNCIACAO', 'DIANA TIEMI YAMAMOTO', 'CAROLINE FREIHAT HENRIQUE DE ALCANTARA SANTANA']
    lista_cargo_senior = ['CLAUDIA CINARE RODRIGUES ETO', 'ROSEMEIRE DIAS FERREIRA', 'JOSE CARVALHO']
    lista_cargo_gerente = ['ROGERIO DE CAMPOS FRIAS']
    lista_cargo_outros = ['OUTROS']

    # FUNÇÃO ATRIBUIR CARGO
    def categorizar_cargo_analista(nome):
        nome = nome.upper()
        if nome in lista_cargo_motor:
            return '6 - MOTOR'
        elif nome in lista_cargo_assistente:
            return '1 - ASSISTENTE'
        elif nome in lista_cargo_junior:
            return '2 - JÚNIOR'
        elif nome in lista_cargo_pleno:
            return '3 - PLENO'
        elif nome in lista_cargo_senior:
            return '4 - SÊNIOR'
        elif nome in lista_cargo_gerente:
            return '5 - GERENTE'
        elif nome in lista_cargo_outros:
            return '7 - OUTROS'
        else:
            return 'ADICIONAR NO DICIONARIO DE NOMES DE ANALISTAS'


    # Aplicando a função de categorizar no DataFrame
    df_resolvido['cargo_analista'] = df_resolvido['analista_tratado'].apply(categorizar_cargo_analista)




    # LISTA DE RAMIFICAÇÕES POR CATEGORIA
    lista_ramificacao_ruim = ['PF SOCIO < 2 ANOS', 'PF CONSORCIO/CONSTRUTORA/SPE/SA', 'PF MESA', 'PF SOCIO PJ', 'PF FUNDACAO < 2 ANOS',
                            'REPROVADO', 'PF MEI', 'PF NATUREZA JURIDICA', 'PF CNAE', 'B - 6', 'B - 9', 'C5 | C1', 'PF BLOQUEIO ALPE',
                            'B - 8', 'B - 11', 'PF CNPJ IRREGULAR', 'PF PEP', 'PF RJ', 'PF CONSORCIO/CONSTRUTORA/SPE', 'PF SOCIO PJ OU < 2 ANOS',
                            'A - E1', 'A - A6', 'A - C2', 'PF JA TEVE ANALISE ANTERIOR ALPE', 'A - A11', 'A - A8']

    lista_ramificacao_medio = ['MESA', 'B - 5', 'B - 7', 'B - 4', 'B - 10', 'A - C1', 'A - B1', 'A - D1', 'A - B8', 'A - A5', 'B - 3',
                            'A - B3', 'A - B2', 'A - B6', 'A - A10', 'A - C6', 'A - C8']

    lista_ramificacao_bom = ['B - 1']

    lista_ramificacao_nan = ['NÃO ATRIBUIDA', 'B - 12', 'A - A12']

    # CONFIRMAR RAMIFICAÇÕES B - 12 e A - A12

    # FUNÇÃO ATRIBUIR CARGO
    def categorizar_ramificacao(nome):
        nome = nome.upper()
        if nome in lista_ramificacao_ruim:
            return '1 - RUIM'
        elif nome in lista_ramificacao_medio:
            return '2 - MÉDIO'
        elif nome in lista_ramificacao_bom:
            return '3 - BOM'
        elif nome in lista_ramificacao_nan:
            return '4 - NÃO ATRIBUIDA'
        else:
            return 'ADICIONAR NO DICIONARIO DE RAMIFICAÇÕES'


    # Aplicando a função de categorizar no DataFrame
    df_resolvido['categoria_ramificacao'] = df_resolvido['ramificacao_motor'].apply(categorizar_ramificacao)



    # PROPOSTAS RÉPLICAS PARA A MESA

    # -> Propostas decididas pelo MOTOR como REPROVADO

    df_motor_reprov = df_resolvido[(df_resolvido['categoria_decisor'] == 'MOTOR') &
                                (df_resolvido['decisao'] == 'REPROVADO')]

    df_motor_reprov_agrup = df_motor_reprov.groupby('cnpj')['data_resolvido'].min().reset_index()

    df_motor_reprov_agrup = pd.merge(df_motor_reprov_agrup, df_resolvido[['cnpj','data_resolvido','ramificacao_motor']], on=['cnpj', 'data_resolvido'], how='left')

    df_motor_reprov_agrup.rename(columns={'data_resolvido': 'primeira_recusa_motor'}, inplace=True)

    df_motor_reprov_agrup['primeira_recusa_motor'] = pd.to_datetime(df_motor_reprov_agrup['primeira_recusa_motor'], errors='coerce')


    # -> Propostas decididas pela MESA, apenas dos casos que tiveram alguma reprova pelo MOTOR

    df_mesa = df_resolvido[(df_resolvido['categoria_decisor'] == 'MESA') & 
                        (df_resolvido['cnpj'].isin(df_motor_reprov_agrup['cnpj']))]

    df_mesa = df_mesa[['cnpj', 'categoria_decisor', 'decisao', 'data_resolvido', 'issue_key']]

    df_mesa['data_resolvido'] = pd.to_datetime(df_mesa['data_resolvido'], errors='coerce')

    df_mesa.reset_index(drop=True, inplace=True)


    # -> Cruzando as bases

    df_propostas_replicas = pd.merge(df_motor_reprov_agrup, df_mesa, on='cnpj', how='left')

    df_propostas_replicas = df_propostas_replicas[df_propostas_replicas['decisao'].notna()]

    df_propostas_replicas.reset_index(drop=True, inplace=True)

    # Filtrando apenas casos que o data_resolvido > primeira_recusa_motor
    df_propostas_replicas_final = df_propostas_replicas[df_propostas_replicas['data_resolvido'] > df_propostas_replicas['primeira_recusa_motor']]


    # Criando flag no df_resolvido

    df_propostas_replicas_final = df_propostas_replicas_final[['issue_key', 'ramificacao_motor']]

    df_propostas_replicas_final.rename(columns={'ramificacao_motor': 'ramificacao_proposta_replica'}, inplace=True)

    df_propostas_replicas_final = df_propostas_replicas_final.drop_duplicates()

    df_resolvido = pd.merge(df_resolvido, df_propostas_replicas_final, on='issue_key', how='left', indicator=True)

    df_resolvido['flag_proposta_replica'] = df_resolvido['_merge'].apply(lambda x: 1 if x == 'both' else 0)

    df_resolvido = df_resolvido.drop(columns=['_merge'])

    df_resolvido = df_resolvido.drop_duplicates()

    # CONVERTENDO COLUNAS DE DATA
    df_resolvido['data_criado'] = pd.to_datetime(df_resolvido['data_criado'], errors='coerce')
    df_resolvido['data_resolvido'] = pd.to_datetime(df_resolvido['data_resolvido'], errors='coerce')
    df_resolvido['data_atualizado'] = pd.to_datetime(df_resolvido['data_atualizado'], errors='coerce')
    df_resolvido['data_disponivel_mesa'] = pd.to_datetime(df_resolvido['data_disponivel_mesa'], errors='coerce')


    # Adicionando colunas de data e hora
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_resolvido['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_resolvido['year'], df_resolvido['month'], df_resolvido['day'] = now.year, now.month, now.day

    # FILTRANDO APENAS APROVADOS E REPROVADOS PARA TRUSTED
    # FILTRANDO APENAS APROVADOS E REPROVADOS PARA TRUSTED E LIMITE SOLICITADO MENOR QUE 1.000.000.000
    df_resolvido = df_resolvido.loc[
        (df_resolvido['decisao'].isin(['APROVADO', 'REPROVADO'])) & 
        (df_resolvido['limite_pedido'] < 1000000000)
    ]

    # SELECIONA AS COLUNAS PARA EXPORTAR
    df_final = df_resolvido[[
        'issue_key', 'politica', 'cnpj', 'raiz_cnpj', 'pgid', 'limite_pedido',
        'limite_aprovado', 'nome_issue', 'nome_vendedor_alpe', 'gerente_tratado',
        'nome_vendedor_fn', 'filial_fn', 'prioridade', 'status', 'decisor','analista_tratado',
        'cargo_analista', 'categoria_decisor', 'decisao', 'parecer', 'ramificacao_motor', 
        'categoria_ramificacao', 'tipo_proposta','flag_proposta_replica', 'data_criado',
        'data_resolvido', 'data_atualizado','data_disponivel_mesa','atualizado_em', 'year', 'month', 'day'
        ]
    ].reset_index(drop=True)


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