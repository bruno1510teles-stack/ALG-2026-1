from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator


default_args = {
    'owner': 'Felipe Ferraz',
    'start_date': days_ago(1),
    'retries': 0,
} 


with DAG(
    dag_id='receita_federal_to_trusted',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    default_args = default_args,
    tags=['etl', 'receita_federal','trusted', 'refined'],
    max_active_runs=1
) as dag:

    cnae = SparkKubernetesOperator(
        task_id='cnae',
        application_file='cnae-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
    )

    empresas = SparkKubernetesOperator(
        task_id='empresas',
        application_file='empresas-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
    )

    estabelecimentos = SparkKubernetesOperator(
        task_id='estabelecimentos',
        application_file='estabelecimentos-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
    )

    motivos = SparkKubernetesOperator(
        task_id='motivos',
        application_file='motivos-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
    )

    municipios = SparkKubernetesOperator(
        task_id='municipios',
        application_file='municipios-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
    )
    naturezas = SparkKubernetesOperator(
        task_id='naturezas',
        application_file='naturezas-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
    )
    paises = SparkKubernetesOperator(
        task_id='paises',
        application_file='paises-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
    )
    qualificacoes = SparkKubernetesOperator(
        task_id='qualificacoes',
        application_file='qualificacoes-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
    )
    simples = SparkKubernetesOperator(
        task_id='simples',
        application_file='simples-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
    )
    socios = SparkKubernetesOperator(
        task_id='socios',
        application_file='socios-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
    )

    pre_filtro = SparkKubernetesOperator(
        task_id='pre_filtro',
        application_file='pre-filtro-spark-app.yaml',
        namespace='spark',
        kubernetes_conn_id='kubernetes_default',
        do_xcom_push=True,
    )

    cnae >> empresas >> estabelecimentos >> motivos >> municipios >> naturezas >> paises >> qualificacoes >> simples >> socios >> pre_filtro