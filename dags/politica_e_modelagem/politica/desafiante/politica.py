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


def executa_politica (access_params=None,  **kwargs):

    ### Configurando configurações necessárias
    # Trino
    conn = connect(
        host='trino.alpe.com.br',
        port='443',
        user='trinodados',
        auth=BasicAuthentication('trinodados', 'hosgzPvuhyXkP<j}RyT+'),
        http_scheme="https",
    )

    # Pegando DF tarefa anterior
    ti = kwargs['ti']
    base_pre_filtro_dict = ti.xcom_pull(task_ids='pre_filtro_task')
    base_pre_filtro = pd.DataFrame(base_pre_filtro_dict)
    
    base_pre_filtro['cnpj_raiz'] = '00000000' + base_pre_filtro['cnpj_raiz'].astype(str)
    base_pre_filtro['cnpj_raiz'] = base_pre_filtro['cnpj_raiz'].str[-8:]

    df_politica_segue = base_pre_filtro.loc[base_pre_filtro['resposta'] == "SEGUE"]


    df_politica_dif_segue = base_pre_filtro[base_pre_filtro['resposta'] != "SEGUE"]
    df_politica_dif_segue['ramificacao_2'] = 'REPROVADO'

    print(f'Quantidade de linhas onde "Resposta" == "segue": {len(df_politica_segue)}')
    print(f'Quantidade de linhas onde "Resposta" != "segue": {len(df_politica_dif_segue)}')


    print('Parte 1 - Pegando info Serasa e Pontualidade...')


    cnpjs = df_politica_segue['cnpj_raiz'].unique()
    ids_query = ', '.join(f"'{cnpj}'" for cnpj in cnpjs)
    ids_query = f"({ids_query})"

    # Caso não retorne nenhum CNPJ como segue ele corrige para não dar erro na query. A partir disso a query retornará com as colunas mas com nenhum registro e o fluxo seguirá normalmente
    if ids_query == "()":
        ids_query = "('')"


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
                            from 
                                deltalakerefined.motor.pontualidade  
                            where 
                                raiz_cnpj in {ids_query}
                        """)

    base_pontualidade = execute_query(conn, query_pontualidade)

    print(f"Quantidade de CNPJs que retornou da base_pontualidade: {base_pontualidade.shape[0]}")


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

    print('Parte 1')

    resultado_politica = resultado_politica.drop('id', axis=1)

    print('Parte 2')
    resultado_politica = resultado_politica.drop_duplicates()

    if resultado_politica.empty:
        print("Nenhum dado encontrado nas tabelas 'com_hp' e 'sem_hp'. O DataFrame está vazio.")

        colunas = ['cnpj_raiz', 'ramificacao_final', 'decisao_final', 'flag_decidido_pelo_motor']
        resultado_politica = pd.DataFrame(columns=colunas)

    else:
        print('Parte 3')
        resultado_politica['ramificacao_final'] = resultado_politica['ramificacao'] + ' | ' + resultado_politica['ramificacao_2']

        print('Parte 4')
        resultado_politica['flag_decidido_pelo_motor'] = True

        print('Parte 5')
        resultado_politica = resultado_politica[['cnpj_raiz', 'ramificacao_final', 'decisao_final', 'flag_decidido_pelo_motor']]

    print('Parte 6')
    pd.set_option('display.max_rows', None)  # Mostra todas as linhas
    pd.set_option('display.max_columns', None)  # Mostra todas as colunas
    pd.set_option('display.width', None)  # Ajusta a largura para que o DataFrame não quebre em várias linhas
    pd.set_option('display.max_colwidth', None)  # Permite exibir o conteúdo completo de cada coluna
    
    # Merge da base inicial com campos filtrados com a base de decisao da politica
    print("base pre_filtro")
    print(base_pre_filtro)
    df_resultado = pd.merge(base_pre_filtro, resultado_politica, on='cnpj_raiz', how='left')
    print(df_resultado)


    print('Parte 7')

    # Tratando base final
    df_resultado['documento_sem_formatacao'] = df_resultado['documento_sem_formatacao'].apply(lambda x: str(x).zfill(14))

    print('Parte 8')

    df_resultado['decisao_final'] = np.where(
        (df_resultado['decisao_final'].isnull()) & (df_resultado['resposta'] == 'SEGUE'),
        'MESA',
        np.where(
            (df_resultado['decisao_final'].isnull()),
            df_resultado['resposta'],
            df_resultado['decisao_final']
        )
    )


    df_resultado.loc[df_resultado['decisao_final'] == 'REPROVADO', 'parecer'] = 'Motor - Recusado, dados analisados fora da politica atual'
    df_resultado.loc[df_resultado['decisao_final'] == 'MESA', 'parecer'] = 'Motor - Direcionar para avaliação da mesa de crédito'
    df_resultado.loc[df_resultado['decisao_final'] == 'mantido', 'parecer'] = 'Motor - Limite mantido'


    if 'ramificacao_pre_filtro' in df_resultado.columns:
        df_resultado['ramificacao_final'] = np.where(
            df_resultado['ramificacao_final'].notna(),  
            df_resultado['ramificacao_final'],          
            np.where(
                df_resultado['ramificacao_pre_filtro'].notna(),  
                df_resultado['ramificacao_pre_filtro'],          
                df_resultado['ramificacao_antifraude']           
            )
        )
    else:
        df_resultado['ramificacao_final'] = np.where(
            df_resultado['ramificacao_final'].notna(),  
            df_resultado['ramificacao_final'],         
            df_resultado['ramificacao_antifraude']      
        )


    print('Parte 9')

    # Criação do mapeamento de pareceres
    parecer_map = {
        "PF 1":"Motor - Recusado, impedido de operar",
        "PF 2":"Motor - Recusado, impedido de operar",
        "PF 3":"Motor - Recusado, MEI",
        "PF MEI":"Motor - Recusado, MEI",
        "PF 4":"Motor - Recusado, CNAE",
        "PF 5":"Motor - Recusado, CNAE",
        "PF CNAE":"Motor - Recusado, CNAE",
        "PF 7":"Motor - Recusado, impedido de operar",
        "PF 9":"Motor - Recusado, impedido de operar",
        "PF 10":"Motor - Recusado, impedido de operar",
        "PF CNPJ IRREGULAR":"Motor - Recusado, impedido de operar",
        "A - A11":"Motor - Recusado, apontamento/score",
        "A - A9":"Motor - Recusado, apontamento/score",
        "A - A8":"Motor - Recusado, apontamento/score",
        "A - A6":"Motor - Recusado, apontamento/score",
        "A - B7":"Motor - Recusado, apontamento/score",
        "A - B5":"Motor - Recusado, apontamento/score",
        "A - B4":"Motor - Recusado, apontamento/score",
        "A - E1":"Motor - Recusado, PD",
        "A - C7":"Motor - Recusado, apontamento/score",
        "A - C5":"Motor - Recusado, apontamento/score",
        "A - C4":"Motor - Recusado, apontamento/score",
        "A - C2":"Motor - Recusado, apontamento/score",
        "B - 11":"Motor - Recusado, apontamento/score",
        "B - 9":"Motor - Recusado, apontamento/score",
        "B - 8":"Motor - Recusado, apontamento/score",
        "B - 6":"Motor - Recusado, apontamento/score",
        "C1 | C1":"Motor - Recusado, apontamento/score",
        "C2 | C2": "Motor - Recusado, apontamento/score",
        "D1 | D1": "Motor - Recusado, apontamento/score",
        "C3 | C1": "Motor - Recusado, apontamento/score",
        "C4 | C2": "Motor - Recusado, apontamento/score",
        "D2 | D1": "Motor - Recusado, apontamento/score",
        "D3 | D1": "Motor - Recusado, apontamento/score",
        "AF - MUDANÇA ENDEREÇO": "Motor - Recusado, risco de fraude",
        "AF - MUDANÇA CIDADE": "Motor - Recusado, risco de fraude",
        "AF - MUDANÇA ESTADO": "Motor - Recusado, risco de fraude",
        "AF - ENDEREÇO IGUAL": "Motor - Recusado, risco de fraude"
    }

    print('Parte 10')

    # Função que retorna o parecer personalizado ou o parecer original se não houver mapeamento
    def parecer_personalizado(row):
        decisao = row['ramificacao_final']
        parecer_atual = row['parecer']
        return parecer_map.get(decisao, parecer_atual)
    
    # Aplica a função ao DataFrame
    df_resultado['parecer'] = df_resultado.apply(parecer_personalizado, axis=1)

    print('Parte 11')


    # Gerando Path
    def gerando_path(row):
           current_date = datetime.now().strftime('%Y-%m-%d')
           return f"{row['cnpj_raiz']}/{current_date}-{row['pgid']}-{row['issue_jira']}"
    # Aplicando o Path
    df_resultado['path_arquivos_minio'] = df_resultado.apply(gerando_path, axis = 1)
    df_resultado['url'] = 'https://minio-datalake.alpe.com.br/raw/browser/analise-credito/' + df_resultado['path_arquivos_minio'] + '/'

    df_resultado['valor_aprovado'] = np.where(
        df_resultado['decisao_final'] == 'APROVADO',
        50000,
        0
    )

    print('Parte 12')

    df_resultado = df_resultado.rename(columns={'documento_sem_formatacao':'cnpj_ec'})

    print('Parte 13')

    resposta_motor_resumida = df_resultado[['issue_jira', 'decisao_final', 'cnpj_ec', 'parecer', 'ramificacao_final', 'url', 'valor_aprovado']].rename(columns={
    'cnpj_ec': 'cnpj_ec',
    'decisao_final': 'resolucao',
    'ramificacao_final': 'ramificacao',
    'url' : 'path_arquivos_minio'
    })


    #depois do path
    resposta_motor_resumida = resposta_motor_resumida[['issue_jira', 'resolucao', 'cnpj_ec', 'valor_aprovado', 'parecer', 'ramificacao', 'path_arquivos_minio']]

    print("Quantidade de CNPJs por ramificação e decisão:")
    print(resposta_motor_resumida.groupby(['issue_jira','cnpj_ec','ramificacao','resolucao'])['cnpj_ec'].size())

    print("Quantidade de CNPJs por decisão:")
    print(resposta_motor_resumida.groupby(['resolucao'])['cnpj_ec'].size())


    print('Exportando o arquivo para o MinIO...')

    # Itera sobre cada combinação de 'issue_jira' e 'CNPJ' no DataFrame
    for _, row in df_resultado.iterrows():
        issue_jira = row['issue_jira']
        cnpj = row['cnpj_ec']
             
        # Gera o nome base do arquivo combinando 'issue_jira' e 'CNPJ'
        file_base_name = 'Resposta_Motor'

        # Filtra o DataFrame resumido e detalhado para o CNPJ específico
        df_detalhado = df_resultado[df_resultado['cnpj_ec'] == cnpj]

        # Adiciona mensagens de log para depuração
        print(f"Processando CNPJ: {cnpj}")
        print(f"Detalhado DF: {df_detalhado.shape}")

        # Gera o caminho de saída usando a coluna 'path'
        file_out_detalhado = f'{row["path_arquivos_minio"]}/{file_base_name}.csv'
              
        # Salva a análise detalhada
        csv_bytes_detalhado = df_detalhado.to_csv(index=False, sep=';').encode('utf-8')
        csv_buffer_detalhado = BytesIO(csv_bytes_detalhado)

        # Salva o arquivo no bucket MinIO usando o caminho gerado
        # # Conectando na refined
        minio_raw = Minio(
            access_params['endpoint_url_raw'],
            access_key=access_params['aws_access_key_id_raw'],
            secret_key=access_params['aws_secret_access_key_raw'],
        )

        BUCKET_SOURCE_RAW = "analise-credito"

        minio_raw.put_object(
            BUCKET_SOURCE_RAW,
            file_out_detalhado,
            data=csv_buffer_detalhado,
            length=len(csv_bytes_detalhado)
    )


    return resposta_motor_resumida.to_dict(orient='records')