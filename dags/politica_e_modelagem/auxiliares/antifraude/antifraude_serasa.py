# Carregando libs
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import os, pytz
from datetime import datetime
import time
import base64
import requests
from airflow.models import Variable


def anti_fraude_serasa (access_params=None,  **kwargs):

   ### Configurando configurações necessárias
    # Trino
    conn = connect(
        host=access_params['trino_endpoint'],
        port=access_params['trino_port'],
        user=access_params['trino_user'],
        auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
        http_scheme="https",
    )


    ### Funções SQL
    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        return pd.DataFrame(rows, columns=columns)


    print('Carregando base da última task...')


    # Pegando DF tarefa anterior
    # Recupera o objeto ti (task instance) via kwargs
    ti = kwargs['ti']
    base_analisar_dict  = ti.xcom_pull(task_ids='pre_filtro_task')
    base_analisar = pd.DataFrame(base_analisar_dict)


    print('Formatando cnpj antes da query...')


    ### Tratando base
    base_analisar['cnpj_sem_formatacao'] = base_analisar['documento_sem_formatacao'].str.zfill(14)
    cnpj_analisar = base_analisar['cnpj_sem_formatacao'].unique()

    cnpj_analisar_str = ', '.join([f"'{cnpj}'" for cnpj in cnpj_analisar])

    print(cnpj_analisar_str)


    print('Carregando dados do Trino...')


    query_antifraude_serasa = f"""
        with consultas_serasa as (
        select 
            h.inquiry_date
            ,h.occurrences
            ,pi2.value "cnpj_sem_formatacao"
            ,re.id
        from postgres.exrp_{Variable.get('STAGE')}_default.report_execution re
            inner join postgres.exrp_{Variable.get('STAGE')}_default.reports rs on rs.id = re.reports_id
            inner join postgres.exrp_{Variable.get('STAGE')}_default.report r on rs.id = r.reports_id
            inner join postgres.exrp_{Variable.get('STAGE')}_default.facts f on f.id = r.facts_id
            inner join postgres.exrp_{Variable.get('STAGE')}_default.inquiry_company_response icr on icr.id = f.inquiry_company_response_id
            inner join postgres.exrp_{Variable.get('STAGE')}_default.quantity q on q.id = icr.quantity_id
            inner join postgres.exrp_{Variable.get('STAGE')}_default.historical h on h.quantity_id = q.id	
            inner join postgres.exrp_{Variable.get('STAGE')}_default.report_involvement ri on ri.report_execution_id = re.id
            inner join postgres.exrp_{Variable.get('STAGE')}_default.party_identification pi2 on pi2.party_id = ri.party_id
        where pi2.value in ({cnpj_analisar_str})
        ),
        base_data as (
            select 
                re.id id
                ,re.created_date data_consulta
                ,rc.json_content
                ,substring(last_re.value,1,8) cnpj_raiz
                ,re.reports_id
            from 
                postgres.exrp_{Variable.get('STAGE')}_default.report_execution re
                inner join postgres.exrp_{Variable.get('STAGE')}_default.report_content rc on rc.id = re.content_id
                inner join (
                    select 
                        min(re.id) re_id
                        ,rc.json_content
                        ,pi2.value
                        ,ROW_NUMBER() OVER (PARTITION BY pi2.value ORDER BY json_content desc) AS rn
                    from postgres.exrp_{Variable.get('STAGE')}_default.report_execution re
                        inner join postgres.exrp_{Variable.get('STAGE')}_default.report_definition rd on rd.id = re.definition_id and rd."type" = 'RELATORIO_AVANCADO_PJ_ANALITICO'
                        inner join postgres.exrp_{Variable.get('STAGE')}_default.report_content rc on rc.id = re.content_id
                        inner join postgres.exrp_{Variable.get('STAGE')}_default.report_involvement ri on ri.report_execution_id = re.id
                        inner join postgres.exrp_{Variable.get('STAGE')}_default.party_identification pi2 on pi2.party_id = ri.party_id
                    where
                        re.resolution = 'DONE'
                    group by  
                        rc.json_content
                        ,pi2.value
                )last_re on last_re.re_id = re.id and rn=1	
        ),
        consulta_mais_recente as (
        select 
        id,
        max(data_consulta) as ultima_consulta
        from base_data
        group by
        id
        )
        select 
            cs.id
            ,inquiry_date as referencia_mes_consulta_serasa
            ,occurrences as qtde_consultas
            ,cnpj_sem_formatacao
            ,ultima_consulta
        from 
        consultas_serasa cs
        inner join consulta_mais_recente cmr on cs.id = cmr.id
        order by inquiry_date
        """

    df_antifraude_serasa = execute_query(conn, query_antifraude_serasa)
    print("Query antifraude serasa carregada com sucesso!!")


    print("Linhas retornadas na base do serasa:")
    print(len(df_antifraude_serasa))


    print('Criando regra de variação das consultas no serasa...')


    # Regra de variação de consultas no serasa
    primeiros_3 = df_antifraude_serasa.groupby('cnpj_sem_formatacao').head(3)
    primeiros_3 = primeiros_3.groupby(['cnpj_sem_formatacao']).agg(
        media_consultas_primeiros_3_meses = ('qtde_consultas', 'mean')
    )

    ultimos_3  = df_antifraude_serasa.groupby('cnpj_sem_formatacao').tail(3)
    ultimos_3  = ultimos_3 .groupby(['cnpj_sem_formatacao']).agg(
        media_consultas_ultimos_3_meses = ('qtde_consultas', 'mean')
    )

    df_antifraude_serasa_final = pd.merge(primeiros_3, ultimos_3, on = 'cnpj_sem_formatacao', how = 'left')
    df_antifraude_serasa_final['variacao'] = df_antifraude_serasa_final['media_consultas_ultimos_3_meses'] / df_antifraude_serasa_final['media_consultas_primeiros_3_meses'] -1

    # Criando Flag
    df_antifraude_serasa_final['flag_antifraude_consultas_serasa'] = np.where(
        (df_antifraude_serasa_final['variacao'] > 2) & (df_antifraude_serasa_final['media_consultas_ultimos_3_meses'] >= 5),
        'Sim',
        'Nao' 
    )


    print('Tratando e gerando tabela final...')



    if df_antifraude_serasa_final.empty:
        print("Nenhum dado encontrado nas tabelas de antifraude serasa. O DataFrame está vazio.")

        df_antifraude_serasa_final = base_analisar
        df_antifraude_serasa_final['cnpj_raiz'] = df_antifraude_serasa_final['cnpj_sem_formatacao'].str.slice(0, 8).str.zfill(8)
        df_antifraude_serasa_final['documento_sem_formatacao'] = df_antifraude_serasa_final['cnpj_sem_formatacao'].str.zfill(14)
        df_antifraude_serasa_final['ramificacao_antifraude_serasa'] = np.nan
        df_antifraude_serasa_final['resposta_antifraude_serasa'] = np.nan

        print(df_antifraude_serasa_final)

    else:
        print('Dados encontrados, politica antifraude serasa iniciando...')
        
        df_antifraude_serasa_final['ramificacao_antifraude_serasa'] = np.nan

        # Criando condições filtro
        condicoes = [
            (df_antifraude_serasa_final['flag_antifraude_consultas_serasa'] == 'Sim', 'AF - CONSULTAS SERASA')
        ]

        for condition, value in condicoes:
            df_antifraude_serasa_final.loc[condition & df_antifraude_serasa_final['ramificacao_antifraude_serasa'].isna(), 'ramificacao_antifraude_serasa'] = value

        df_antifraude_serasa_final.loc[df_antifraude_serasa_final['ramificacao_antifraude_serasa'].isna(), 'ramificacao_antifraude_serasa'] = 'AF SERASA SEGUE'

        # Criando Resposta
        response_map = {
            'REPROVADO': ['AF - CONSULTAS SERASA'],
            'SEGUE': ['AF SERASA SEGUE']
        }

        # Aplicar as respostas
        for response, values in response_map.items():
            df_antifraude_serasa_final.loc[df_antifraude_serasa_final['ramificacao_antifraude_serasa'].isin(values), 'resposta_antifraude_serasa'] = response

        print(df_antifraude_serasa_final)

    print(base_analisar)

    df = pd.merge(base_analisar, df_antifraude_serasa_final[['cnpj_sem_formatacao', 'ramificacao_antifraude_serasa', 'resposta_antifraude_serasa']], 
              on = ['cnpj_sem_formatacao'], how = 'left')


    df = df.reset_index(drop=True)

    # Tratando os casos que não retornaram informação das bases de antifraude serasa
    df.loc[df['ramificacao_antifraude_serasa'].isna(), 'ramificacao_antifraude_serasa'] = 'AF SERASA SEM INFO'
    df.loc[df['resposta_antifraude_serasa'].isna(), 'resposta_antifraude_serasa'] = 'MESA'


    return df.to_dict(orient='records')