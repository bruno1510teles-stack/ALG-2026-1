from datetime import datetime, timezone
import json
import pendulum
from airflow.decorators import dag, task
from analises_credito.kafka_producer import Kafka
from scripts.analises_credito.error_warning import erro


default_args = {
    'owner': 'Rafael Leite'
}

@dag(
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=-1),
    schedule=None,
    description='DAG consulta trino e faz upload do arquivo para minio',
    catchup=False,
    on_failure_callback=erro
)
def fail():

    @task
    def fail_task():
        raise ValueError("Erro")
    
    fail_task()

dag_instance = fail()
