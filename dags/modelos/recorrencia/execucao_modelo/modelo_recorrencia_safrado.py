### IMPORTANDO BIBLIOTECAS
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from io import BytesIO
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from deltalake import write_deltalake, DeltaTable
from airflow.utils.log.logging_mixin import LoggingMixin
import joblib


def clusters_modelo_recorrencia (access_params=None, **kwargs):

    # Carregar pipeline salvo
    pipeline = joblib.load("/opt/airflow/dags/repo/dags/modelos/recorrencia/execucao_modelo/kmeans_pipeline_completo.pkl")


    ### CONECTANDO COM O TRINO
    conn = connect(
        host=access_params['trino_endpoint'],
        port=access_params['trino_port'],
        user=access_params['trino_user'],
        auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
        http_scheme="https",
    )

    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        return pd.DataFrame(rows, columns=columns)
    

    ### DATA REF
    query_data_ref = f"""
                        select distinct data_ref 
                        from deltalakerefined.modelos.auxiliar_modelo_recorrencia
                        """

    data_ref_df = execute_query (conn, query_data_ref)


    datas_safras = data_ref_df['data_ref'].unique()
    datas_safras.sort()

    print(datas_safras)



    # Lista para acumular os resultados
    resultados = []

    for data in datas_safras:

        data_str = data.strftime('%Y-%m-%d')

        print('Executando fechamento para a data:')
        print(data_str)

        ### PUBLICO DA ANÁLISE (CLIENTES QUE JÁ TIVERAM NA DEVIDA SAFRA)
        
        query_modelo = f"""
                        select 
                            * 
                        from deltalakerefined.modelos.auxiliar_modelo_recorrencia
                        where data_ref = date '{data_str}'
        """
        
        df_modelo = execute_query (conn, query_modelo)

        ### APLICANDO O PIPELINE
        df_safra = df_modelo.copy()
        df_safra["cluster_kmeans"] = pipeline.predict(df_safra)
        
        # Adiciona a referência da safra
        df_safra["data_ref"] = data  # aqui `data` é o objeto datetime do loop
        
        ### ACUMULANDO RESULTADO
        resultados.append(df_safra)


    # CONCTENA TUDO NO FINAL
    df_resultado = pd.concat(resultados, ignore_index=True)


    # FILTRANDO COLUNAS
    df_resultado = df_resultado[['data_ref','raiz_cnpj', 'cluster_kmeans']]


    map_clusters = {
    0: "CLIENTES NOVOS",
    1: "RECORRÊNCIA MÉDIA",
    2: "RETOMARAM COMPRAS",
    3: "RECORRÊNCIA ALTA",
    4: "RECORRÊNCIA BAIXA"
    }

    # APLICA MAPEAMENTO
    df_resultado["cluster_nome"] = df_resultado["cluster_kmeans"].map(map_clusters)


    # CRIANDO CHAVE
    df_resultado["data_ref"] = df_resultado["data_ref"].astype(str)

    df_resultado["chave"] = df_resultado["data_ref"] + "_" + df_resultado["raiz_cnpj"].astype(str)

    # REORDENANDO COLUNAS
    cols = ["data_ref", "chave"] + [c for c in df_resultado.columns if c not in ["data_ref", "chave"]]
    df_resultado = df_resultado[cols]



    # Adicionando colunas de data e hora
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_resultado['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_resultado['year'], df_resultado['month'], df_resultado['day'] = now.year, now.month, now.day


    # Configurações para acesso ao MinIO
    logger = LoggingMixin().log 

    try:
        logger.info("Iniciando salvamento das informações")
        
        storage_options = {
            "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_refined'],
            "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_refined'],
            "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_refined']}",
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
        }


        # Definindo o caminho e salvando no MinIO
        BUCKET_SOURCE_REFINED = "modelos"
        FOLDER_DESTINATION_REFINED = "recorrencia"

        write_deltalake(
            f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}", 
            df_resultado, 
            partition_by=["year", "month", "day"],
            storage_options=storage_options,
            mode="overwrite"
            # overwrite_schema=True
    )
        logger.info("Salvamento concluído com sucesso.")
        
    except Exception as e:
        logger.error(f"Erro ao salvar as informações: {str(e)}")