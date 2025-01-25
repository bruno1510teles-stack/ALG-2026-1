# Carregando libs
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import os, pytz
import time
import base64
import requests
import boto3


def executa_politica_v5 (access_params=None,  **kwargs):

    # Pegando DF tarefa anterior
    # Recupera o objeto ti (task instance) via kwargs
    ti = kwargs['ti']
    base_analisar_dict = ti.xcom_pull(task_ids='pre_filtro_task')
    base_analisar = pd.DataFrame(base_analisar_dict)

    df_politica = base_analisar.copy()

    df_politica['cnpj_raiz'] = '00000000' + df_politica['cnpj_raiz'].astype(str)
    df_politica['cnpj_raiz'] = df_politica['cnpj_raiz'].str[-8:]

    df_politica_segue = df_politica.loc[df_politica['resposta'] == "SEGUE"]
    df_politica_dif_segue = df_politica[df_politica['resposta'] != "SEGUE"]

    print(f'Quantidade de linhas onde "Resposta" == "segue": {len(df_politica_segue)}')
    print(f'Quantidade de linhas onde "Resposta" != "segue": {len(df_politica_dif_segue)}')


    print('Parte 1 - Pegando info Serasa e Pontualidade...')


    cnpjs = df_politica_segue['cnpj_raiz'].unique()
    ids_query = ', '.join(f"'{cnpj}'" for cnpj in cnpjs)
    ids_query = f"({ids_query})"

    conn = connect(
        host='trino.alpe.com.br',
        port='443',
        user='trinodados',
        auth=BasicAuthentication('trinodados', 'hosgzPvuhyXkP<j}RyT+'),
        http_scheme="https",
    )

    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        
        return pd.DataFrame(rows, columns=columns)


    query_serasa = (f"""
            with 
        total_restritivos_pj as (
    select 
    t1.org_id as id,
    t1.data_consulta,
    t1.cnpj_raiz,
    sum(valor_total) as "TOTAL RESTRITIVOS"
    from (
    select distinct
        org.id org_id,
        date(split_part(org.data_hora_consulta, ' ', 1)) AS data_consulta,
        org.cnpj_raiz,
        remp.id,
        remp.grupo_ocorrencia,
        remp.quantidade_ocorrencia,
        remp.ano_mes_primeiro,
        remp.ano_mes_ultimo,
        remp.moeda,
        remp.codigo_natureza,
        remp.fonte,
        remp.titular_pendencia,
        coalesce(remp.valor_total, 0) as valor_total
    from
        deltalaketrusted.serasa.organizacoes org
    left join deltalaketrusted.serasa.resumo_restritivo remp
        on
        remp.id = org.id
        and remp.titular_pendencia = CONCAT('0',
        org.cnpj_raiz)
    where
        org.cnpj_raiz in {ids_query}
    )t1
    group by
    t1.org_id,
    t1.data_consulta,
    t1.cnpj_raiz
    )
    ,
        qtde_cheque_pj as (
    select 
    t1.org_id as id,
    sum(t1."QTD CHEQUE") as "QTD CHEQUE"
    from (
    select distinct
        org.id org_id,
        org.cnpj_raiz,
        remp.id,
        remp.grupo_ocorrencia,
        remp.quantidade_ocorrencia,
        remp.ano_mes_primeiro,
        remp.ano_mes_ultimo,
        remp.moeda,
        remp.codigo_natureza,
        remp.fonte,
        remp.titular_pendencia,
        coalesce(remp.quantidade_ocorrencia, 0) as "QTD CHEQUE"
    from
        deltalaketrusted.serasa.organizacoes org
    left join deltalaketrusted.serasa.resumo_restritivo remp
        on
        remp.id = org.id
        and remp.titular_pendencia = CONCAT('0',
        org.cnpj_raiz)
    where
        org.cnpj_raiz in {ids_query}
        and remp.grupo_ocorrencia = 'CHEQUE'
    ) t1
    group by
    t1.org_id
    )
    ,
        qtde_cheque_pf as (
    select 
    t1.org_id as id,
    sum(t1."CHEQUE PF") as "CHEQUE PF"
    from (
    select distinct
        socios.id org_id,
        rsocio.id,
        rsocio.grupo_ocorrencia,
        rsocio.quantidade_ocorrencia,
        rsocio.ano_mes_primeiro,
        rsocio.ano_mes_ultimo,
        rsocio.moeda,
        rsocio.codigo_natureza,
        rsocio.fonte,
        rsocio.titular_pendencia,
        coalesce(rsocio.quantidade_ocorrencia, 0) as "CHEQUE PF"
    from
        deltalaketrusted.serasa.socios socios
    left join deltalaketrusted.serasa.resumo_restritivo rsocio
        on
        rsocio.id = socios.id
        and rsocio.titular_pendencia = socios.documento_socio
    where
        rsocio.grupo_ocorrencia = 'CHEQUE') t1
    group by
    t1.org_id
    )
    ,
        total_restritivos_socio as (
    select 
    t1.org_id as id,
    sum(valor_total) as "RESTRITIVOS PF"
    from (
    select distinct
        socios.id org_id,
        rsocio.id,
        rsocio.grupo_ocorrencia,
        rsocio.quantidade_ocorrencia,
        rsocio.ano_mes_primeiro,
        rsocio.ano_mes_ultimo,
        rsocio.moeda,
        rsocio.codigo_natureza,
        rsocio.fonte,
        rsocio.titular_pendencia,
        coalesce(rsocio.valor_total, 0) as valor_total
    from
        deltalaketrusted.serasa.socios socios
    left join deltalaketrusted.serasa.resumo_restritivo rsocio
        on
        rsocio.id = socios.id
        and rsocio.titular_pendencia = socios.documento_socio
    )t1
    group by
    t1.org_id
    )
    ,
        score_pj as (
    select 
        sc.id, sc.valor_score as "Score Positivo PJ",
        case when sc.probabilidade_inadimplencia_mensagem  = 'EMPRESA CORPORATE PLUS RECOMENDA-SE CONSULTAR CREDIT RATING SERASA EXPERIAN' then 1 else 0 end as grande_empresa
    from
        deltalaketrusted.serasa.score sc
    ),
        consulta_mais_recente AS (
    select
        org.cnpj_raiz,
        MAX(date(split_part(org.data_hora_consulta, ' ', 1))) AS consulta_mais_recente
    from 
        deltalaketrusted.serasa.organizacoes org 
    where 
        org.cnpj_raiz in {ids_query}
    group by
        org.cnpj_raiz
    ),
    RankedSocios AS (
        SELECT 
            org.id, 
            so.documento_socio, 
            so.percentual_capital,
            ROW_NUMBER() OVER (PARTITION BY org.id ORDER BY so.percentual_capital DESC, so.documento_socio) AS rn
        FROM 
            deltalaketrusted.serasa.organizacoes org
        LEFT JOIN 
            deltalaketrusted.serasa.socios so ON org.id = so.id
        where 
            org.cnpj_raiz in {ids_query}
    )
    select 
        trp.id,
        trp.data_consulta,
        trp.cnpj_raiz,
        score."Score Positivo PJ",
        score.grande_empresa,
        coalesce(trp."TOTAL RESTRITIVOS", 0) as "TOTAL RESTRITIVOS",
        coalesce(cpj."QTD CHEQUE", 0) as "QTD CHEQUE",
        coalesce(cpf."CHEQUE PF", 0) as "CHEQUE PF",
        coalesce(trs."RESTRITIVOS PF", 0) as "RESTRITIVOS PF",
        rs.documento_socio as "CPF do Principal Socio"
    from 
        total_restritivos_pj trp
    left join 
        qtde_cheque_pj cpj
        on trp.id = cpj.id
    left join 
        qtde_cheque_pf cpf
        on trp.id = cpf.id
    left join 
        total_restritivos_socio trs 
        on trp.id = trs.id
    left join 
        score_pj score
        on trp.id = score.id
    inner join 
        consulta_mais_recente cmr
        on trp.cnpj_raiz = cmr.cnpj_raiz and trp.data_consulta = cmr.consulta_mais_recente
    left join 
        RankedSocios rs ON trp.id = rs.id
    where 
        rs.rn = 1
        """)

    base_retorno_serasa = execute_query(conn, query_serasa)

    print(f"Quantidade de CNPJs que retornou da base_retorno_serasa: {base_retorno_serasa.shape[0]}")


    query_pontualidade = (f"""
                        select
                            raiz_cnpj as cnpj_raiz,
                            pontualidade
                        from deltalakerefined.motor.pontualidade  
                        """)

    base_pontualidade = execute_query(conn, query_pontualidade)

    print(f"Quantidade de CNPJs que retornou da base_retorno_serasa: {base_pontualidade.shape[0]}")


    print('Parte 2 - Criando Flags e Campos necessarios para Politica V5...')


    base_analisar = pd.merge(base_retorno_serasa, base_pontualidade, on = ['cnpj_raiz'], how = 'left')

    base_analisar['valor_total_restritivos'] = base_analisar['TOTAL RESTRITIVOS'] + base_analisar['RESTRITIVOS PF']

    base_analisar['flag_restritivo'] = base_analisar['valor_total_restritivos'].apply(lambda x: 1 if x > 10 else 0)

    base_analisar['grande_empresa'] = base_analisar['grande_empresa'].fillna(0).astype(int)
    base_analisar['Score Positivo PJ'] = base_analisar['Score Positivo PJ'].fillna(0).astype(int)

    base_analisar.rename(
        columns={
            "Score Positivo PJ": "score_positivo_pj",
            "grande_empresa": "empresa_grande",
            "TOTAL RESTRITIVOS": "total_restritivos",
            "QTD CHEQUE": "qtd_cheque",
            "CHEQUE PF": "qtd_cheque_pf",
            "RESTRITIVOS PF": "total_restritivos_pf",
            "CPF do Principal Socio": "cpf_socio_principal"
        },
        inplace=True,
    )


    print('Parte 3 - Dividindo base COM HP e SEM HP...')


    sem_hp = base_analisar[base_analisar['pontualidade'].isna()].reset_index(drop=True)
    com_hp = base_analisar[base_analisar['pontualidade'].notna()].reset_index(drop=True)


    print('Parte 4 - Aplicando Politica...')


    # Tratando casos sem informações de pagamento

    # Função para calcular a ramificação e a decisão final
    def aplica_regras_sem_hp(row):
        # Caso o porte seja 'CORPORATE'
        if row['empresa_grande'] == 1:
            row['ramificacao'] = 'B6'
            row['ramificacao_2'] = 'B3'
            row['decisao_final'] = 'MESA'
        else:
            # Verificar se há restritivo
            if row['flag_restritivo'] == 0:
                # Se não há restritivo, aplicar a lógica de score
                if row['score_positivo_pj'] > 900:
                    row['ramificacao'] = 'A6'
                    row['ramificacao_2'] = 'A1'
                    row['decisao_final'] = 'APROVADO'
                elif row['score_positivo_pj'] > 700:
                    row['ramificacao'] = 'A7'
                    row['ramificacao_2'] = 'A2'
                    row['decisao_final'] = 'APROVADO'
                elif row['score_positivo_pj'] > 600:
                    row['ramificacao'] = 'A8'
                    row['ramificacao_2'] = 'A3'
                    row['decisao_final'] = 'APROVADO'
                elif row['score_positivo_pj'] > 316:
                    row['ramificacao'] = 'B4'
                    row['ramificacao_2'] = 'B1'
                    row['decisao_final'] = 'MESA'
                else:
                    row['ramificacao'] = 'C5'
                    row['ramificacao_2'] = 'C1'
                    row['decisao_final'] = 'REPROVADO'
            else:
                # Caso tenha restritivo
                if row['valor_total_restritivos'] < 500000:
                    # Se o restritivo for menor que 500k
                    if row['score_positivo_pj'] > 316:
                        row['ramificacao'] = 'B5'
                        row['ramificacao_2'] = 'B2'
                        row['decisao_final'] = 'MESA'
                    else:
                        row['ramificacao'] = 'C6'
                        row['ramificacao_2'] = 'C2'
                        row['decisao_final'] = 'REPROVADO'
                else:
                    # Se o restritivo for maior que 500k
                    row['ramificacao'] = 'D4'
                    row['ramificacao_2'] = 'D1'
                    row['decisao_final'] = 'REPROVADO'
        return row

    # Aplicar a função em cada linha do DataFrame 'sem_hp'
    sem_hp = sem_hp.apply(aplica_regras_sem_hp, axis=1)

 

    # Tratando casos com informações de pagamento

    # Função para calcular a ramificação e a decisão final
    def aplica_regras_com_hp(row):
        # Caso o porte seja 'CORPORATE'
        if row['empresa_grande'] == 1:
            row['ramificacao'] = 'B3'
            row['ramificacao_2'] = 'B3'
            row['decisao_final'] = 'MESA'     
        else:
            if row['pontualidade'] >= 99:
                if row['flag_restritivo'] == 0:
                    if row['score_positivo_pj'] > 900:
                        row['ramificacao'] = 'AA'
                        row['ramificacao_2'] = 'AA'
                        row['decisao_final'] = 'APROVADO'
                    elif row['score_positivo_pj'] > 700:
                        row['ramificacao'] = 'A1'
                        row['ramificacao_2'] = 'A1'
                        row['decisao_final'] = 'APROVADO'
                    elif row['score_positivo_pj'] > 500:
                        row['ramificacao'] = 'A2'
                        row['ramificacao_2'] = 'A2'
                        row['decisao_final'] = 'APROVADO'
                    elif row['score_positivo_pj'] > 316:
                        row['ramificacao'] = 'B1'
                        row['ramificacao_2'] = 'B1'
                        row['decisao_final'] = 'MESA'
                    else:
                        row['ramificacao'] = 'C1'
                        row['ramificacao_2'] = 'C1'
                        row['decisao_final'] = 'REPROVADO'
                else:
                    if row['valor_total_restritivos'] < 500000:
                        if row['score_positivo_pj'] > 316:
                            row['ramificacao'] = 'B2'
                            row['ramificacao_2'] = 'B2'
                            row['decisao_final'] = 'MESA'
                        else:
                            row['ramificacao'] = 'C2'
                            row['ramificacao_2'] = 'C2'
                            row['decisao_final'] = 'REPROVADO'
                    else:
                        row['ramificacao'] = 'D1'
                        row['ramificacao_2'] = 'D1'
                        row['decisao_final'] = 'REPROVADO'                    
            elif row['pontualidade'] >= 40:
                if row['flag_restritivo'] == 0 and row['pontualidade'] > 90:
                    if row['score_positivo_pj'] > 900:
                        row['ramificacao'] = 'A3'
                        row['ramificacao_2'] = 'A1'
                        row['decisao_final'] = 'APROVADO'
                    elif row['score_positivo_pj'] > 700:
                        row['ramificacao'] = 'A4'
                        row['ramificacao_2'] = 'A2'
                        row['decisao_final'] = 'APROVADO'
                    elif row['score_positivo_pj'] > 600:
                        row['ramificacao'] = 'A5'
                        row['ramificacao_2'] = 'A3'
                        row['decisao_final'] = 'APROVADO'
                    elif row['score_positivo_pj'] > 316:
                        row['ramificacao'] = 'B3'
                        row['ramificacao_2'] = 'B1'
                        row['decisao_final'] = 'MESA'
                    else:
                        row['ramificacao'] = 'C3'
                        row['ramificacao_2'] = 'C1'
                        row['decisao_final'] = 'REPROVADO'
                else:
                    if row['valor_total_restritivos'] < 500000:
                        if row['score_positivo_pj'] > 316:
                            row['ramificacao'] = 'B4'
                            row['ramificacao_2'] = 'B2'
                            row['decisao_final'] = 'MESA'
                        else:
                            row['ramificacao'] = 'C4'
                            row['ramificacao_2'] = 'C2'
                            row['decisao_final'] = 'REPROVADO'
                    else:
                        row['ramificacao'] = 'D2'
                        row['ramificacao_2'] = 'D1'
                        row['decisao_final'] = 'REPROVADO'
            else: 
                row['ramificacao'] = 'D3'
                row['ramificacao_2'] = 'D1'
                row['decisao_final'] = 'REPROVADO'
        return row

    # Aplicar a função em cada linha do DataFrame 'sem_hp'
    com_hp = com_hp.apply(aplica_regras_com_hp, axis=1)

    print('Tratamentos finais...')

    resultado_politica = pd.concat([com_hp, sem_hp], ignore_index=True)
    resultado_politica = resultado_politica.drop('id', axis=1)
    resultado_politica = resultado_politica.drop_duplicates()
    resultado_politica['ramificacao_final'] = resultado_politica['ramificacao'] + ' | ' + resultado_politica['ramificacao_2']
    resultado_politica['flag_decidido_pelo_motor'] = True
    resultado_politica = resultado_politica[['cnpj_raiz', 'ramificacao_final', 'decisao_final', 'flag_decidido_pelo_motor']]

    # Merge da base inicial com campos filtrados com a base de decisao da politica
    df_politica_filtrado = df_politica[['cnpj_raiz', 'razao_social', 'documento_sem_formatacao', 'ramificacao_pre_filtro', 'resposta']]
    df_resultado = pd.merge(df_politica_filtrado, resultado_politica, on='cnpj_raiz', how='left')

    # Tratando base final
    df_resultado['ramificacao_final'] = df_resultado['ramificacao_final'].fillna(df_resultado['ramificacao_pre_filtro'])
    df_resultado['decisao_final'] = df_resultado['decisao_final'].fillna(df_resultado['resposta'])
    df_resultado['flag_decidido_pelo_motor'] = df_resultado['flag_decidido_pelo_motor'].fillna(False)
    df_resultado = df_resultado.drop(['ramificacao_pre_filtro', 'resposta'], axis=1)

    df_resultado['documento_sem_formatacao'] = df_resultado['documento_sem_formatacao'].apply(lambda x: str(x).zfill(14))


    print('Exportando o arquivo para o MinIO...')

    # Exportando arquivo para o MinIO, a fim de guardarmos o historico...

    # Gerando o arquivo Excel em memória
    file_buffer = BytesIO()
    df_resultado.to_excel(file_buffer, index=False, engine="openpyxl")
    file_buffer.seek(0)

    # Configuração da conexão S3/MinIO
    storage_options = {
        "aws_access_key_id": 'nr0qPLaAcdCtt7lAV4oa',
        "aws_secret_access_key": 'GRA8FxnVMy7pGDvKP1wZK2nPOC3vP7F1AvH2u3Ch',
        "endpoint_url": "https://api-trusted.alpe.com.br",
        "region_name": "us-east-1",
    }


    # Conectando ao S3/MinIO
    s3 = boto3.client("s3", **storage_options)

    # Definindo bucket e caminho de destino
    BUCKET_SOURCE_TRUSTED = "pre-aprovado-lote"
    FOLDER_DESTINATION_TRUSTED = "aprovados"

    # Gerando a data atual no formato desejado (ex: '13012025')
    current_date = datetime.now().strftime("%d%m%Y")

    # Definindo o nome do arquivo com a data automatizada
    file_name = f"resposta_politica_v5_{current_date}.xlsx"

    # Definindo o caminho do objeto no S3
    object_key = f"{FOLDER_DESTINATION_TRUSTED}/{file_name}"

    # Fazendo o upload do arquivo Excel para o S3
    s3.upload_fileobj(file_buffer, BUCKET_SOURCE_TRUSTED, object_key)