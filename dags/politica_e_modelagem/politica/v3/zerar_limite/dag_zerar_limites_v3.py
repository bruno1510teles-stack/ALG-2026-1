### Importando Libs necessárias
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
from time import sleep


### Importando scripts necessários
from politica_e_modelagem.politica.v3.zerar_limite import captura_proposta
from politica_e_modelagem.politica.v3.zerar_limite import executa_politica
from politica_e_modelagem.politica.v3.zerar_limite import envio_kafka_task    


### Parâmetros de acesso
access_params = {          
    "endpoint_url_trusted": Variable.get("MINIO_TRUSTED_ENDPOINT"),
    "aws_access_key_id_trusted": Variable.get("MINIO_TRUSTED_ACCESS_KEY"),
    "aws_secret_access_key_trusted": Variable.get("MINIO_TRUSTED_SECRET_KEY"),
    "endpoint_url_refined": Variable.get("MINIO_REFINED_ENDPOINT"),
    "aws_access_key_id_refined": Variable.get("MINIO_REFINED_ACCESS_KEY"),
    "aws_secret_access_key_refined": Variable.get("MINIO_REFINED_SECRET_KEY"),
    "endpoint_url_raw": Variable.get("MINIO_RAW_ENDPOINT"),
    "aws_access_key_id_raw": Variable.get("MINIO_RAW_ACCESS_KEY"),
    "aws_secret_access_key_raw": Variable.get("MINIO_RAW_SECRET_KEY"),
    "trino_endpoint": Variable.get("TRINO_ENDPOINT"),
    "trino_port": Variable.get("TRINO_PORT"),
    "trino_user": Variable.get("TRINO_USER"),
    "trino_password": Variable.get("TRINO_PASSWORD"),
    "opdb_bucket": Variable.get("OPDB_BUCKET"),
    "stage": Variable.get('STAGE'),
    "kafka_url": Variable.get('KAFKA_DATALAKE_ENDPOINT'),
    "exrep_url": Variable.get('EXREP_BASE_URL'),
    "exrep_client_id": Variable.get('EXREP_CLIENT_ID'),
    "exrep_client_secret": Variable.get('EXREP_CLIENT_SECRET'),
    "keycloack_token_url": Variable.get('KEYCLOAK_TOKEN_URL')
    }


# Definindo defaults
default_args = {
    "owner": "Vinicius Moraes",
    "retries": 0,
}


# Função para processar as propostas recebidas

def processar_proposta(**kwargs):
    # Capturando os parâmetros enviados via conf
    conf = kwargs.get('dag_run').conf
    issue_jira = conf.get('issue_key')
    CNPJ = conf.get('payer_identification')
    inad_alpe = conf.get('inad')
    pgid = conf.get('payee_pgid')
    nome_issue = conf.get('summary')

    # Criando um dicionário com os dados recebidos para simular o DataFrame
    dados = {
        'issue_jira' : [issue_jira],
        'CNPJ': [CNPJ],
        'inad_alpe': [inad_alpe],
        'pgid': [pgid],
        'nome_issue': [nome_issue]
    }

    print(dados)
    print(dados['CNPJ'])
    print(dados['issue_jira'])

    # Convertendo o dicionário em DataFrame para aplicar as transformações
    df = pd.DataFrame(dados)
    print(df)

    # Passo 1: Remover a máscara de valor e converter para numérico
    if df['inad_alpe'].notna().any():
        df['inad_alpe'] = df['inad_alpe'].replace({'R\$ ': '', '\.': ''}, regex=True)
        df['inad_alpe'] = pd.to_numeric(df['inad_alpe'], errors='coerce')  # Converte para float, substituindo erros por NaN

    # Passo 3: Criar as colunas de flag com base na lógica fornecida
    df['inad_alpe_flag'] = df['inad_alpe'].apply(lambda x: 'SIM' if pd.notna(x) and x > 0 else 'NAO')

    # Passo 4: Deixar o nome padrão
    # Remover as colunas originais 'limite_alpe' e 'inad_alpe'
    df.drop(columns=['inad_alpe'], inplace=True)

    # Renomear as colunas de flag para os nomes originais
    df.rename(columns={'inad_alpe_flag': 'inad_alpe'}, inplace=True)

    # Filtrar o DataFrame para manter apenas as linhas onde 'nome_issue' contenha a palavra 'LOTE'
    #df = df[~df['nome_issue'].str.contains('LOTE', case=False, na=False)]

    # Exibir o DataFrame resultante (para fins de debug, pode ser removido)
    print(df)

    return df.to_dict(orient='records')


# Definindo a DAG
with DAG(
    dag_id='politica_v_3',
    start_date=days_ago(1),
    schedule_interval='0 13 * * 1',  # Rodar todas as segundas-feiras às 10:00
    default_args=default_args,
    tags=['politica_v3', 'zerar_limite'] # DAG só será acionada manualmente pela API
) as dag:

    # Definindo o task que processa a proposta
    captura_proposta = PythonOperator(
        task_id='captura_proposta',
        python_callable=processar_proposta,
        provide_context=True  # Habilita o envio do contexto (incluindo conf)
    )

    # Execução da política
    executa_politica = PythonOperator(
        task_id = "executa_politica",
        python_callable = executa_politica.execucao_politica,
        op_kwargs = {'access_params': access_params},
        provide_context = True
    )

    # Enviando dados para o Kafka
    enviar_kafka = PythonOperator(
        task_id = "envio_kafka_task",
        python_callable = envio_kafka_task.envio_kafka,
        op_kwargs = {'access_params': access_params},
        provide_context = True,
    )

    # Definindo a ordem de execução das tasks
    captura_proposta >> executa_politica >> enviar_kafka