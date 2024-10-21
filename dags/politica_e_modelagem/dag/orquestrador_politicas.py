from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime

def identify_policy(**kwargs):
    # Suponha que as propostas venham com algum parâmetro ou ID que permite identificar a política
    proposal_id = kwargs['dag_run'].conf.get('proposal_id')
    
    # Aqui você faria a lógica de mapeamento para identificar qual política (DAG) aplicar
    if proposal_id in [1]:
        return 'politica_v_1'
    elif proposal_id in [2]:
        return 'politica_v_2'
    elif proposal_id in [3]:
        return 'politica_v_3'
    else:
        return 'policy_default_dag'

default_args = {
    'start_date': datetime(2023, 10, 1),
    "owner": "Felipe Ferraz"
}

with DAG(dag_id='orquestrador_politicas', schedule_interval=None, default_args=default_args, catchup=False) as dag:
    
    # Passo 1: Identificar qual política deve ser executada
    policy_decision = PythonOperator(
        task_id='identify_policy',
        python_callable=identify_policy,
        provide_context=True
    )

    # Passo 2: Acionar a DAG correspondente à política
    trigger_policy_dag = TriggerDagRunOperator(
        task_id='trigger_policy_dag',
        trigger_dag_id="{{ task_instance.xcom_pull(task_ids='identify_policy') }}", # Aciona a DAG retornada
        conf={"proposal_id": "{{ dag_run.conf['proposal_id'] }}"} # Passa parâmetros, se necessário
    )

    policy_decision >> trigger_policy_dag