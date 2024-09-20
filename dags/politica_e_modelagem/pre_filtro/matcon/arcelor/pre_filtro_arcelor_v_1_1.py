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


def analise_pre_filtro(access_params=None,  **kwargs):

    ### Configurando configurações necessárias
    # Trino
    conn = connect(
        host=access_params['trino_endpoint'],
        port=access_params['trino_port'],
        user=access_params['trino_user'],
        auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
        http_scheme="https",
    )
    # Pegando DF tarefa anterior
    # Recupera o objeto ti (task instance) via kwargs
    ti = kwargs['ti']
    base_analisar_dict  = ti.xcom_pull(task_ids='captura_proposta')
    base_analisar = pd.DataFrame(base_analisar_dict)

    ### Tratando base
    base_analisar['CNPJ'] = base_analisar['CNPJ'].str.zfill(14)
    base_analisar['cnpj_raiz'] = base_analisar['CNPJ'].str.slice(0, 8).str.zfill(8)
    print(f"Quantidade de CNPJs na base_analisar: {base_analisar.shape[0]}")

    cnpjs = base_analisar['CNPJ'].unique()
    cnpjs_raiz = base_analisar['cnpj_raiz'].str.slice(0, 8).unique()
    print(f"Quantidade de CNPJs na cnpjs_raiz: {cnpjs_raiz.shape[0]}")

    # Criar uma string formatada para a cláusula IN
    ids_query = ', '.join(f"'{cnpj}'" for cnpj in cnpjs_raiz)
    print(ids_query)
    ids_query = f"({ids_query})"
    


    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        return pd.DataFrame(rows, columns=columns)

    # Base auxiliar CNAE
    query_cnae = """
        SELECT * 
        FROM miniorefined.dimensao.cnaes_aceitos
    """
    aux_cnae = execute_query(conn, query_cnae)

    # Base auxiliar Natureza Jurídica
    query_nat_ju = """
        SELECT * 
        FROM miniorefined.dimensao.natureza_juridica
    """
    aux_nat_ju = execute_query(conn, query_nat_ju)

    # Base estabelecimentos
    query_estabelecimentos = f"""
        SELECT DISTINCT est.documento_sem_formatacao 
        FROM deltalaketrusted.receita_federal.estabelecimentos est
        WHERE est.cnpj_raiz IN {ids_query} AND is_matriz = true
    """
    base_analisar_raiz = execute_query(conn, query_estabelecimentos)

    ## Criar uma string formatada para a cláusula IN
    base_analisar_raiz['cnpj_raiz'] = base_analisar_raiz['documento_sem_formatacao'].str.slice(0, 8).str.zfill(8)

    df = pd.DataFrame()

    # Cria um cursor e executa a query
    cur = conn.cursor()

    for index, row in base_analisar_raiz.iterrows():
        cnpj_completo = row['documento_sem_formatacao']
        cnpj_raiz = row['cnpj_raiz'] 

        query = (f"""

            with limite as (select 
            cnpj_raiz, case when sum(limite_atribuido) > 0 
            then True 
            else False  
            end as limite_alpe 
            from 
                postgres.ccred_schema_prd_default.vw_limite_sacado_v3 
            where 
                cnpj_raiz = '{cnpj_raiz}'
                and cedente_principal
            group by 
                cnpj_raiz
                            )
            select 
                pre.cnpj_raiz cnpj_raiz,
                pre.documento_sem_formatacao,
                pre.razao_social,
                pre.cod_cnae,
                est.cnae_secundaria, 
                pre.cod_natureza_juridica,
                CAST(emp.codigo_porte_empresa AS DECIMAL) AS codigo_porte_empresa,
                emp.capital_social_empresa as "Capital Social",
                pre.idade,
                pre.situacao_cadastral situacao_cadastral,
                pre.idade_socio,
                pre.tem_socio_pj,
                pre.is_mei,
                pre.tem_pep,
                pre.situacao_especial,
                pre.data_ref_receita,
                lim.limite_alpe
            from 
                deltalakerefined.motor.pre_filtro pre
            left join deltalaketrusted.teste_joao.empresas emp on emp.cnpj_raiz = pre.cnpj_raiz --and pre.data_ref_receita = emp.data_ref
            left join limite lim on lim.cnpj_raiz = pre.cnpj_raiz 
            left join deltalaketrusted.teste_joao.estabelecimentos est on est.documento_sem_formatacao = pre.documento_sem_formatacao
            where pre.documento_sem_formatacao = '{cnpj_completo}'

            """)

        # Executa a query
        cur.execute(query)

        # Obtém os resultados
        rows = cur.fetchall()

        # Para pegar o nome das colunas
        columns = [desc[0] for desc in cur.description]

        # Converte os resultados em um DataFrame temporário
        df_temp = pd.DataFrame(rows, columns=columns)

        # Concatenar os resultados temporários no DataFrame final
        df = pd.concat([df, df_temp], ignore_index=True)

    # Fecha o cursor e a conexão
    cur.close()
    conn.close()

    # Exibe o DataFrame final com os resultados acumulados
    print(df)

    ### Olhando para os CNAE's secundários também
    # Passo 1: Criar uma coluna com todos os CNAEs (principal + secundários)
    df['todos_cnaes'] = df.apply(lambda row: [row['cod_cnae']] + (row['cnae_secundaria'].split(',') if pd.notna(row['cnae_secundaria']) else []), axis=1)
    # Passo 2: Explodir o DataFrame para que cada CNAE fique em uma linha separada
    df_exploded = df.explode('todos_cnaes')
    # Passo 3: Fazer o merge para validar os CNAEs
    df_validado = df_exploded.merge(aux_cnae, on = ['cod_cnae'], how = 'inner')
    # Passo 4: Consolidar o resultado para manter uma linha por CNPJ e verificar se ao menos um CNAE foi aceito
    df = df_validado.groupby('documento_sem_formatacao').agg({
        'cnpj_raiz': 'first',  # Mantém o primeiro valor da coluna cnpj_raiz (assumindo que seja o mesmo para cada grupo)
        'cod_cnae': 'first',  # Mantém o CNAE principal
        'todos_cnaes': lambda x: ','.join(x),  # Junta os CNAEs novamente
        'cnae_aceito': lambda x: 'SIM' if 'SIM' in x.values else 'NAO',  # Se qualquer um for 'SIM', aceita
        'razao_social': 'first',  # Mantém o primeiro valor da coluna razao_social
        'cod_natureza_juridica': 'first',  # Mantém o primeiro valor da coluna cod_natureza_juridica
        'codigo_porte_empresa': 'first',  # Mantém o primeiro valor da coluna codigo_porte_empresa
        'Capital Social': 'first',  # Mantém o primeiro valor da coluna Capital Social
        'idade': 'first',  # Mantém o primeiro valor da coluna idade
        'situacao_cadastral': 'first',  # Mantém o primeiro valor da coluna situacao_cadastral
        'idade_socio': 'first',  # Mantém o primeiro valor da coluna idade_socio
        'tem_socio_pj': 'first',  # Mantém o primeiro valor da coluna tem_socio_pj
        'is_mei': 'first',  # Mantém o primeiro valor da coluna is_mei
        'tem_pep': 'first',  # Mantém o primeiro valor da coluna tem_pep
        'situacao_especial': 'first',  # Mantém o primeiro valor da coluna situacao_especial
        'data_ref_receita': 'first',  # Mantém o primeiro valor da coluna data_ref_receita
        'limite_alpe': 'first'  # Mantém o primeiro valor da coluna limite_alpe
    }).reset_index()  # Reseta o índice para retornar um DataFrame regular

    ### Concatenando base principal(import)
    df = df.merge(base_analisar[['cnpj_raiz', 'issue_jira', 'inad_alpe', 'pgid']], on = ['cnpj_raiz'], how = 'left')

    ### Cruzando DF
    df = df.merge(aux_nat_ju, on = ['cod_natureza_juridica'], how = 'inner')
    
    # Criando função para verificar se é SPE, Consorcio ou Construtora
    def spe_consorcio_construtora(data_frame):
        # Verificação SPE
        is_spe = (data_frame['cod_natureza_juridica'] == '2062') & (
            data_frame['razao_social'].str.startswith("SPE ") | 
            data_frame['razao_social'].str.endswith(" SPE"))
        
        # Verificação Consórcio
        is_consorcio = (data_frame['cod_natureza_juridica'].isin(['1210', '1228', '2151', '2283', '2291']) & (
            data_frame['razao_social'].str.startswith("CONSORCIO ") | 
            data_frame['razao_social'].str.endswith(" CONSORCIO")))
        
        # Verificação Construtora
        is_construtora = (data_frame['cod_natureza_juridica'].isin(['2062', '2135']) &
                           data_frame['cod_cnae'].isin(['3011301', '3011302', '3012100', '4120400', '4211101', '4212000', '4221901', '4221902', '4221904', '4222701', '4223500', '4299501']))

        return (is_spe | is_consorcio | is_construtora)

    # Aplicando Função
    df['is_spe_consorcio_construtora'] = spe_consorcio_construtora(df)
    
    df['ramificacao_pre_filtro'] = np.nan

    # Dicionário para mapear condições a valores de 'ramificacao_pre_filtro'
    conditions = [
        (df['situacao_cadastral'] != 'ATIVA', 'PF 1'),
        (df['situacao_especial'] == 'RECUPERACAO JUDICIAL', 'PF 2'),
        ((df['tem_pep'] == 'True') | (df['tem_pep'] == True), 'PF 7'),
        ((df['limite_alpe'] == True) | (df['limite_alpe'] == 'True'), 'PF 11'),
        ((df['is_mei'] == True) | (df['is_mei'] == 'True'), 'PF 3'),
        (df['cnae_aceito'] == 'NAO', 'PF 4'),
        (df['nat_ju_aceita'] == 'NAO', 'PF 5'),
        (df['is_spe_consorcio_construtora'], 'PF 6'),
        (((df['idade_socio'].notna()) & (df['idade_socio'] < 2)) | (df['tem_socio_pj'] == True), 'PF 8'),
        (df['idade'] < 2, 'PF 9'),
        (df['inad_alpe'] == 'SIM', 'PF 10')
    ]
    # Aplicar condições
    for condition, value in conditions:
        df.loc[condition & df['ramificacao_pre_filtro'].isna(), 'ramificacao_pre_filtro'] = value

    # Atribuir 'PF 12' para os que não se encaixam em nenhuma das condições anteriores
    df.loc[df['ramificacao_pre_filtro'].isna(), 'ramificacao_pre_filtro'] = 'PF 12'

    # Criando Resposta
    response_map = {
        'REPROVADO': ['PF 1', 'PF 2', 'PF 3', 'PF 4', 'PF 5', 'PF 7', 'PF 9', 'PF 10'],
        'MESA': ['PF 6', 'PF 8', 'PF 11'],
        'SEGUE': ['PF 12']
    }
    # Aplicar as respostas
    for response, values in response_map.items():
        df.loc[df['ramificacao_pre_filtro'].isin(values), 'resposta'] = response

    # Definindo a Política e Versão
    df['politica'] = 'Arcelor Mittal'
    df['versao_motor'] = '1.1'

    # Printando resultado
    print(f"Demonstrativo relação pré-filtro: {df.groupby(['issue_jira', 'documento_sem_formatacao', 'ramificacao_pre_filtro'])['documento_sem_formatacao'].size()}")


    ### Salvando DF para utilizar na próxima tarefa da DAG
    return df.to_dict(orient='records')