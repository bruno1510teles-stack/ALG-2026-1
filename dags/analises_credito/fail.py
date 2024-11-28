from datetime import datetime
import json
import pendulum
from airflow.decorators import dag, task
from analises_credito.kafka_producer import Kafka

def erro(context):
    # Obter informações do contexto da falha
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    dag_run_id = context['dag_run'].run_id

    # Corpo da mensagem de erro
    errorBody = {
        "@timestamp": datetime.now().isoformat(),
        "@metadata": {
            "beat": "filebeat",
            "type": "_doc",
            "version": "8.10.4"
        },
        "log.level": "ERROR",
        "message": f"Erro na execução da DAG {dag_id}, Task {task_id}, Run ID {dag_run_id}",
        "error.message": f"Erro durante a execução da DAG {dag_id}",
        "dag_id": dag_id,
        "task_id": task_id,
        "dag_run_id": dag_run_id
    }

    # Converte o dicionário para JSON string e depois para bytes
    error_message = json.dumps(errorBody).encode('utf-8')  # Convertido para bytes

    # Envia mensagem para o Kafka
    Kafka.send(
        topic="prd.default.jira-connector.v1.logs.create.out",
        key="teste",  # Certifique-se de que também seja uma string ou bytes
        value=error_message
    )

default_args = {
    'owner': 'Rafael Leite'
}

@dag(
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=-1),
    schedule=None,
    description='DAG consulta trino e faz upload do arquivo para minio',
    catchup=False,
    on_failure_callback=erro  # Passa a função sem parênteses
)
def fail():

    @task
    def fail_task():
        raise ValueError("Erro")
    
    fail_task()

dag_instance = fail()
