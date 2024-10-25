### Importando Libs necessárias
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
from time import sleep
from datetime import timedelta
import requests


### Importando scripts necessários
from politica_e_modelagem.auxiliares.jira import import_base_jira_agendado
#from politica_e_modelagem.pre_filtro.matcon.arcelor import pre_filtro_arcelor_v_1_1
from politica_e_modelagem.pre_filtro import pre_filtro_v_2
from politica_e_modelagem.auxiliares.serasa import execucao_chamada_serasa
from politica_e_modelagem.modelo.matcon.arcelor import execucao_modelo_arcelor_v_1_0
from politica_e_modelagem.politica.matcon.arcelor import politica_arcelor_v_1_1
from politica_e_modelagem.auxiliares.kafka import execucao_envio_kafka


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

def notificar_falha_teams(context):
    url = "https://yandehbr.webhook.office.com/webhookb2/3efc9ab8-aba8-4150-8e68-864d086592a3@fe284b6f-c6d2-4028-badb-7d0c22aef0ae/IncomingWebhook/2bb511bca72643d58ea858c433be3aec/e3ad1a1a-7716-40ee-ab81-0f05650df5dc/V2AAjaUAPO15qUofSpSzGh6PW4gkg2FJypyvorUwW89eU1"
    mensagem = {
        "title": "Falha na DAG - politica_v_2",
        "text": f"Falha na DAG - politica_v_2: {context['task_instance'].dag_id} na task: {context['task_instance'].task_id} verificar URGENTE!!"
    }
    requests.post(url, json=mensagem)

### Definindo defaults
default_args = {
    "owner": "Felipe Ferraz",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notificar_falha_teams
}


# # Função para processar as propostas recebidas
# def processar_proposta(**kwargs):
#     # Capturando os parâmetros enviados via conf
#     conf = kwargs.get('dag_run').conf
#     issue_jira = conf.get('issue_key')
#     CNPJ = conf.get('payer_identification')
#     inad_alpe = conf.get('inad')
#     pgid = conf.get('payee_pgid')
#     nome_issue = conf.get('summary')

    # # Criando um dicionário com os dados recebidos para simular o DataFrame
    # dados = {
    #     'issue_jira' : [issue_jira],
    #     'CNPJ': [CNPJ],
    #     'inad_alpe': [inad_alpe],
    #     'pgid': [pgid],
    #     'nome_issue': [nome_issue]
    # }

    # # Convertendo o dicionário em DataFrame para aplicar as transformações
    # df = pd.DataFrame(dados)

    # # Passo 1: Remover a máscara de valor e converter para numérico
    # if df['inad_alpe'].notna().any():
    #     df['inad_alpe'] = df['inad_alpe'].replace({'R\$ ': '', '\.': ''}, regex=True)
    #     df['inad_alpe'] = pd.to_numeric(df['inad_alpe'], errors='coerce')  # Converte para float, substituindo erros por NaN

    # # Passo 3: Criar as colunas de flag com base na lógica fornecida
    # df['inad_alpe_flag'] = df['inad_alpe'].apply(lambda x: 'SIM' if pd.notna(x) and x > 0 else 'NAO')

    # # Passo 4: Deixar o nome padrão
    # # Remover as colunas originais 'limite_alpe' e 'inad_alpe'
    # df.drop(columns=['inad_alpe'], inplace=True)

    # # Renomear as colunas de flag para os nomes originais
    # df.rename(columns={'inad_alpe_flag': 'inad_alpe'}, inplace=True)

    # # Filtrar o DataFrame para manter apenas as linhas onde 'nome_issue' contenha a palavra 'LOTE'
    # df = df[~df['nome_issue'].str.contains('LOTE', case=False, na=False)]


    # # Exibir o DataFrame resultante (para fins de debug, pode ser removido)
    # print(df)

    # return df.to_dict()

# # Definindo a DAG
# with DAG(
#     dag_id='politica_credito_arcelor_v.1.1',
#     start_date=days_ago(1),
#     schedule_interval=None,
#     default_args=default_args,
#     tags=['politica_arcelor_v.1.1']  # DAG só será acionada manualmente pela API
# ) as dag:

#     # Definindo o task que processa a proposta
#     captura_proposta = PythonOperator(
#         task_id='captura_proposta',
#         python_callable=processar_proposta,
#         provide_context=True  # Habilita o envio do contexto (incluindo conf)
#     )


# Definindo a DAG
with DAG(
    dag_id='politica_v_2',
    start_date=days_ago(1),
    schedule_interval='*/30 * * * *',
    default_args=default_args,
    tags=['politica_arcelor'],
    max_active_runs=1  # Apenas uma execução ativa
) as dag:

    # Definindo o task que processa a proposta
    captura_proposta = PythonOperator(
        task_id='captura_proposta',
        python_callable=import_base_jira_agendado.base_analisar,
        provide_context=True,  # Habilita o envio do contexto (incluindo conf)
        execution_timeout=timedelta(minutes=3)
    )    
    # Definindo o task de pre filtro
    pre_filtro = PythonOperator(
        task_id="pre_filtro_task",
        python_callable=pre_filtro_v_2.analise_pre_filtro,
        op_kwargs={'access_params': access_params},
        provide_context=True,
        execution_timeout=timedelta(minutes=3)
    )

    # Definindo o task que faz a chamada do serasa
    serasa = PythonOperator(
        task_id="serasa_task",
        python_callable=execucao_chamada_serasa.chamando_serasa,
        op_kwargs={'access_params': access_params},
        provide_context=True,
        execution_timeout=timedelta(minutes=3)
    )

    # Execução do modelo
    modelo = PythonOperator(
        task_id="modelo_task",
        python_callable=execucao_modelo_arcelor_v_1_0.execucao_modelo,
        op_kwargs={'access_params': access_params},
        provide_context=True,
        execution_timeout=timedelta(minutes=3)
    )

    # Execução da política
    politica = PythonOperator(
        task_id="politica_task",
        python_callable=politica_arcelor_v_1_1.execucao_politica,
        op_kwargs={'access_params': access_params},
        provide_context=True,
        execution_timeout=timedelta(minutes=3)
    )

    # Enviando dados para o Kafka
    enviar_kafka = PythonOperator(
        task_id="envio_kafka_task",
        python_callable=execucao_envio_kafka.envio_kafka,
        op_kwargs={'access_params': access_params},
        provide_context=True,
        execution_timeout=timedelta(minutes=3)
    )

    # Definindo o sleep para processo de dados no data lake
    aguarde = PythonOperator(
        task_id="wait_1_minute",
        python_callable=lambda: sleep(1020),  # Espera por 1 minuto (ajustado de 15 minutos)
    )

    # Definindo a ordem de execução das tasks
    captura_proposta >> pre_filtro >> serasa >> aguarde >> modelo >> politica >> enviar_kafka