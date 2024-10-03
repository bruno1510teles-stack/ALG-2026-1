from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta

default_args = {
    'owner': 'Rafael Leite',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
def getParams(issue):
    print(issue)

@dag(
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    description='DAG principal que aciona outras DAGs de forma dinâmica com base em um parâmetro',
    catchup=False,
)
def master_dag_trigger():

    @task()
    def start_task():
        print("DAG principal iniciada")

    @task()
    def trigger_dag(**kwargs):
        # Obtém o parâmetro que define a DAG filha e os parâmetros da execução
        issue = kwargs['dag_run'].conf.get('issue', None)
        #params =
        getParams(issue)

        if not issue:
            raise ValueError("Issue não fornecida na execução do Orquestrador.")
        
        return 
        # Cria o operador que dispara a DAG filha de forma genérica
        trigger = TriggerDagRunOperator(
            task_id=f'trigger_{dag_to_trigger}',  # Nome único para a task com base na DAG
            trigger_dag_id=dag_to_trigger,  # DAG filha a ser acionada
            conf= None ,#"issue_key": "{{ $json.issue.key }}", "summary" : "{{ $('Jira Trigger').item.json.issue.fields.summary }}", "payer_identification" : "{{ $node["Jira Trigger"].json["issue"]["fields"]["customfield_13729"] }}", "payee_pgid" : "{{ $node["Jira Trigger"].json["issue"]["fields"]["customfield_13739"] }}", "minio_url" : "{{ $node["Jira Trigger"].json["issue"]["fields"]["customfield_13806"] }}", "inad" : "{{ $node["Jira Trigger"].json["issue"]["fields"]["customfield_13808"] }}",  # Parâmetros dinâmicos para a DAG filha
            wait_for_completion=False  # Espera pela conclusão da DAG filha
        )
        trigger.execute(context={})  # Executa a DAG filha

    @task()
    def end_task():
        print("DAG principal finalizada")

    # Definindo a ordem de execução
    start = start_task()
    trigger_dag = trigger_dynamic_dag()
    end = end_task()

    start >> trigger_dag >> end


# Instancia a DAG
master_dag = master_dag_trigger()
