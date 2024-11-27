
# Importando bibliotecas

import pandas as pd
import numpy as np
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from minio.error import S3Error
from io import BytesIO
from requests.auth import HTTPBasicAuth
from deltalake import write_deltalake, DeltaTable
from datetime import datetime, timezone, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin

# Conexão do banco de dados

def merge_propostas_boletos(access_params=None, **kwargs):
    conn = connect(
        host='trino.alpe.com.br',
        port='443',
        user='trinodados',
        auth=BasicAuthentication('trinodados', 'hosgzPvuhyXkP<j}RyT+'),
        http_scheme="https",
    )

    # Definindo a execução da query

    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução

        return pd.DataFrame(rows, columns=columns)

    query_jira_propostas = f""" select * from deltalakerefined.jira.propostas """

    query_vop_vendermais = f""" select 	
                                    cnpj_sacado,

                                    round(sum(vop), 2) as vop,
                                    round(sum(valor_desagio), 2) as valor_desagio,
                                    round(sum(vop_a_vencer), 2) as vop_a_vencer,
                                    round(sum(vop_performado), 2) as vop_performado,
                                    round(sum(vencido), 2) as vencido,
                                    
                                    round(sum(vop_over_15), 2) as vop_over_15,
                                    round(sum(vop_over_30), 2) as vop_over_30,
                                    round(sum(vop_over_60), 2) as vop_over_60,
                                    round(sum(vop_over_90), 2) as vop_over_90,		
                                    round(avg(prazo_medio), 2) as prazo_medio,
                                    
                                    round(sum(vop_over15_mob2), 2) as vop_over15_mob2,
                                    round(sum(vop_over15_mob3), 2) as vop_over15_mob3,
                                    round(sum(vop_over30_mob1), 2) as vop_over30_mob1,
                                    round(sum(vop_over30_mob2), 2) as vop_over30_mob2,
                                    round(sum(vop_over30_mob3), 2) as vop_over30_mob3,
                                    round(sum(vop_over30_mob4), 2) as vop_over30_mob4,
                                    round(sum(vop_over30_mob5), 2) as vop_over30_mob5,
                                    round(sum(vop_over30_mob6), 2) as vop_over30_mob6,
                                    
                                    round(sum(vop_over60_mob1), 2) as vop_over60_mob1,
                                    round(sum(vop_over60_mob2), 2) as vop_over60_mob2,
                                    round(sum(vop_over60_mob3), 2) as vop_over60_mob3,
                                    round(sum(vop_over60_mob4), 2) as vop_over60_mob4,
                                    round(sum(vop_over60_mob5), 2) as vop_over60_mob5,
                                    round(sum(vop_over60_mob6), 2) as vop_over60_mob6,
                                    
                                    round(sum(vop_over90_mob1), 2) as vop_over90_mob1,
                                    round(sum(vop_over90_mob2), 2) as vop_over90_mob2,
                                    round(sum(vop_over90_mob3), 2) as vop_over90_mob3,
                                    round(sum(vop_over90_mob4), 2) as vop_over90_mob4,
                                    round(sum(vop_over90_mob5), 2) as vop_over90_mob5,
                                    round(sum(vop_over90_mob6), 2) as vop_over90_mob6			
                            from deltalakerefined.payments.vop_vendermais
                            group by cnpj_sacado 
                        """

    df_propostas = execute_query(conn, query_jira_propostas) 
    df_vop_vendermais = execute_query(conn, query_vop_vendermais)

    vop_vendermais = df_vop_vendermais.copy()

    # Garantindo que o CNPJ seja tratado corretamente
    vop_vendermais['cnpj_sacado_raiz'] = vop_vendermais["cnpj_sacado"].str[:8].astype("object")

    # Removendo a coluna auxiliar
    vop_vendermais.drop(columns=["cnpj_sacado"], inplace=True)

    # Identificando colunas numéricas para o agrupamento
    numerical_columns = vop_vendermais.select_dtypes(include=['number']).columns.tolist()

    # Agrupando apenas as colunas numéricas
    vop_vendermais = vop_vendermais.groupby('cnpj_sacado_raiz')[numerical_columns].sum().reset_index()

    # Garantindo que as colunas informativas sejam tratadas como strings
    informative_columns = ['politica_desc', 'tipo_proposta', 'ramificacao_motor_desc', 'pgid']
    for col in informative_columns:
        df_propostas[col] = df_propostas[col].astype(str)

    # Tratamento adicional para pgid como UPPERCASE
    df_propostas['pgid'] = df_propostas['pgid'].str.upper()

    # Criando a coluna cnpj_sacado_raiz
    df_propostas['cnpj_sacado_raiz'] = df_propostas["cnpj"].str[:8].astype("object")

    # Filtrando propostas aprovadas
    propostas = df_propostas.query("status_decisao == 'Aprovado' and limite_aprovado > 0")[
        ['nome_decisor', 'cnpj_sacado_raiz', 'data_criado', 'hora_criado'] + informative_columns
    ].reset_index(drop=True)

    # Convertendo data e hora para datetime
    propostas["data_hora_decisao"] = pd.to_datetime(propostas["data_criado"].astype(str) + " " + propostas["hora_criado"])

    # Removendo colunas auxiliares de data e hora
    propostas.drop(columns=["data_criado", "hora_criado"], inplace=True)

    # Pegando o responsável pela proposta
    min_data = propostas.loc[propostas.groupby("cnpj_sacado_raiz")["data_hora_decisao"].idxmin()].reset_index(drop=True)

    # Fazendo o merge para adicionar a decisão inicial
    propostas = propostas.merge(min_data, on=["cnpj_sacado_raiz", "data_hora_decisao"], how="left", suffixes=("", "_min"))

    # Criando a flag para indicar o decisor
    propostas["flag_decisor"] = propostas["nome_decisor_min"].notnull().astype(int)

    # Removendo coluna auxiliar
    propostas.drop(columns=["nome_decisor_min", "data_hora_decisao"], inplace=True)

    # Excluindo linhas duplicadas
    propostas.drop_duplicates(inplace=True)

    # Validando quantidade de linhas após filtro e deduplicação
    print(f"Linhas originais de propostas: {df_propostas.shape[0]}")
    print(f"Linhas após filtro e deduplicação: {propostas.shape[0]}")

    # Merge final das bases
    base_final = pd.merge(propostas, vop_vendermais, on='cnpj_sacado_raiz', how='left').reset_index(drop=True)

    # Garantindo que as colunas informativas permaneçam strings no DataFrame final
    for col in informative_columns:
        base_final[col] = base_final[col].astype(str)

    # Ajuste de flag_decisor e preenchimento de valores
    base_final.loc[base_final["flag_decisor"] != 1, base_final.columns.difference(["flag_decisor", "cnpj_sacado_raiz", "nome_decisor"])] = 0
    base_final.fillna(0, inplace=True)

    # Validando número final de linhas
    print(f"Linhas finais na base: {base_final.shape[0]}")

    # Exibindo a base final
    print(base_final.head())


    # Configurações para acesso ao MinIO
    logger = LoggingMixin().log 

    try:
        logger.info("Iniciando salvamento das informações")
        
        storage_options = {
    		"AWS_ACCESS_KEY_ID": "d8jOuN46ckGNsr6zzpyw",
            "AWS_SECRET_ACCESS_KEY": "gnyBdrsoDrRcM9ln0QO83Nw8I4TlOFDOI4J9QDKc",
            "AWS_ENDPOINT_URL": "https://api-refined.alpe.com.br",
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
        }

        # Definindo o caminho e salvando no MinIO
        BUCKET_SOURCE_REFINED = "jira"
        FOLDER_DESTINATION_REFINED = "propostas_boletos_aux_vop"

        write_deltalake(
            f"s3a://{BUCKET_SOURCE_REFINED}/{FOLDER_DESTINATION_REFINED}", 
            base_final, 
            partition_by=["year", "month", "day"],
            storage_options=storage_options,
            mode="overwrite"
            # overwrite_schema=True
        )
        
        logger.info("Salvamento concluído com sucesso.")

    except Exception as e:
        logger.error(f"Erro ao salvar as informações: {str(e)}")