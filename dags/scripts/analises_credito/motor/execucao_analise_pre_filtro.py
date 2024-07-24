# Carregando libs
import pandas as pd
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
from trino.dbapi import connect
from trino.auth import BasicAuthentication


def analise_pre_filtro(access_params=None):

    # VARIAVEIS DE DOS ARQUIVOS
    BUCKET_SOURCE_REFINED = "motor"
    FOLDER_SOURCE_REFINED = 'analise_credito/in/a_processar'
    FOLDER_DESTINATION_REFINED = 'analise_credito/auxiliar'

    # Conectando na refined
    client = Minio(
        'api-refined.alpe.com.br',
        access_key = '0FKu1vkOJbq0K4C0qRuF',
        secret_key = 'PIqXSinLX2q9XTvGsVrw5Z5jzyuBl7ng7hIq62oA',
    )

    # BAIXANDO ARQUIVO A SER ANALISADO
    file = client.get_object(bucket_name=BUCKET_SOURCE_REFINED, object_name=f'{FOLDER_SOURCE_REFINED}/BASE_PRE_FILTRO.csv')

    base_analisar = pd.read_csv(BytesIO(file.data), dtype=str)

    cnpjs = base_analisar['CNPJ'].unique()
    cnpjs_raiz = base_analisar['CNPJ'].str.slice(0, 8).unique()
    ## Criar uma string formatada para a cláusula IN
    ids_query = ', '.join(f"'{cnpj}'" for cnpj in cnpjs_raiz)
    ids_query = f"({ids_query})"
    
    # Configura a conexão
    conn = connect(
        host='trino.alpe.com.br',
        port='443',
        user='trinodados',
        auth=BasicAuthentication('trinodados', 'hosgzPvuhyXkP<j}RyT+'),
        http_scheme="https",
    )

    # Cria um cursor e executa a query
    # Base auxiliar CNAE
    cur = conn.cursor()

    query = (f"""
        select 
            *
        from miniorefined.dimensao.cnaes_aceitos

        """)

    cur.execute(query)

    # Obtém os resultados
    rows = cur.fetchall()

    # Fecha o cursor e a conexão
    cur.close()
    conn.close()

    # Para pegar o nome das colunas, você pode usar cur.description
    columns = [desc[0] for desc in cur.description]
    aux_cnae = pd.DataFrame(rows, columns=columns)


    # Base auxiliar Natureza Jurídica
    cur = conn.cursor()
    query = (f"""
        select 
            *
        from miniorefined.dimensao.natureza_juridica
        """)

    cur.execute(query)

    # Obtém os resultados
    rows = cur.fetchall()

    # Fecha o cursor e a conexão
    cur.close()
    conn.close()

    # Para pegar o nome das colunas, você pode usar cur.description
    columns = [desc[0] for desc in cur.description]
    aux_nat_ju = pd.DataFrame(rows, columns=columns)

    # Cria um cursor e executa a query
    cur = conn.cursor()
    query = (f"""
    select 
        est.documento_sem_formatacao 
    from 
        miniotrusted.receita_federal.estabelecimentos est
    where
        est.cnpj_raiz in {ids_query} and is_matriz = true
        """)

    cur.execute(query)

    # Obtém os resultados
    rows = cur.fetchall()

    # Fecha o cursor e a conexão
    cur.close()
    conn.close()

    # Para pegar o nome das colunas, você pode usar cur.description
    columns = [desc[0] for desc in cur.description]
    base_analisar_raiz = pd.DataFrame(rows, columns=columns)

    ## Criar uma string formatada para a cláusula IN
    cnpjs = base_analisar_raiz['documento_sem_formatacao'].unique()
    ids_query = ', '.join(f"'{cnpj}'" for cnpj in cnpjs)
    ids_query = f"({ids_query})"
    
    # Cria um cursor e executa a query
    cur = conn.cursor()
    query = (f"""

    with
        tem_pep as (select
            distinct
            cnpj_raiz,
            true as tem_pep
        from miniotrusted.pessoas_e_organizacoes.pep pep
        join miniotrusted.receita_federal.socios socios on replace(replace(documento, '.', ''), '-', '') = socios.documento_socio and pep.nome = socios."nome/razao_social"
        where socios.data_ref = (select max(data_ref) data_ref from miniotrusted.receita_federal.socios)
        and pep.data_ref = (select max(data_ref) data_ref from miniotrusted.pessoas_e_organizacoes.pep)
        ),
        
        venc as (
            SELECT
                DISTINCT s.numero_cnpj_sacado, 'SIM' AS inad_alpe
            FROM
                postgres.ccred_schema_prd_default.boleto_titulo bt
                LEFT JOIN postgres.ccred_schema_prd_default.boleto_titulo_endosso bte ON bt.id = bte.boleto_titulo_id_endossado
                LEFT JOIN postgres.ccred_schema_prd_default.boleto_titulo bt2 ON bte.boleto_titulo_id = bt2.id AND bt2.codigo_empresa = 3
                LEFT JOIN postgres.ccred_schema_prd_default.sacado s ON bt.codigo_sacado = s.codigo_sacado
            WHERE
                bt.titulo_pagamento
                AND bt.excluido != true
                AND bt.codigo_estagio_titulo IN (5, 6)
                AND bt.data_efetivacao IS NOT NULL
                AND bt.codigo_cedente NOT IN (12, 188, 6910, 14099, 40585, 99241, 101880, 13974, 14688, 105372)
                AND bt.status_titulo = 'VENCIDO'
        ),    
        
        limi as (
            SELECT
                DISTINCT cnpj_raiz, True AS possui_limite
            FROM
                postgres.ccred_schema_prd_default.vw_limite_sacado_v3
            WHERE cedente_principal = true
                AND sumarizado = true
                AND limite_atribuido >= 0
        ) ,
        
        socio as (select
            cnpj_raiz,
            MAX(data_entrada_sociedade) as mais_recente,
            CASE 
                WHEN 
                    MAX(CASE WHEN identificador_socio = 'PESSOA JURÍDICA' THEN 1 ELSE 0 END) = 1 
                THEN true 
                ELSE false 
                END as tem_socio_pj,
                max(data_ref) data_ref 
            from miniotrusted.receita_federal.socios sc
        group by cnpj_raiz
        )

        select
            est.cnpj_raiz cnpj_raiz,
            est.documento_sem_formatacao documento_sem_formatacao,
            emp.razao_social,
            est.cnae_principal cod_cnae,
            emp.natureza_juridica cod_natureza_juridica,
            emp.codigo_porte_empresa,
            (date_diff('day', date(est.data_inicio_atividade), date(now())) / 365.00) AS idade,
            est.situacao_cadastral situacao_cadastral,
            (date_diff('day', date(s.mais_recente), date(now())) / 365.00) AS idade_socio,
            tem_socio_pj,
            sim.is_mei is_mei,
            coalesce(tem_pep, false) tem_pep,
            COALESCE(venc.inad_alpe, 'NAO') as inad_alpe,
            COALESCE(limi.possui_limite, False) as limite_alpe,
            est.situacao_especial,
            est.data_ref data_ref_receita
        from miniotrusted.receita_federal.estabelecimentos est
        left join miniotrusted.receita_federal.empresas emp on emp.cnpj_raiz = est.cnpj_raiz and est.data_ref = emp.data_ref
        left join miniotrusted.receita_federal.simples sim on sim.cnpj_raiz = est.cnpj_raiz and est.data_ref = sim.data_ref
        left join miniotrusted.receita_federal.naturezas_juridicas natjur on natjur.codigo = emp.natureza_juridica and est.data_ref = natjur.data_ref
        left join miniotrusted.receita_federal.cnaes on cnaes.codigo = est.cnae_principal and est.data_ref = cnaes.data_ref
        left join tem_pep on tem_pep.cnpj_raiz = est.cnpj_raiz
        left join venc ON est.documento_sem_formatacao = venc.numero_cnpj_sacado
        left join limi ON est.cnpj_raiz = limi.cnpj_raiz
        left join socio s on est.cnpj_raiz = s.cnpj_raiz and est.data_ref = s.data_ref

        where est.documento_sem_formatacao in {ids_query}

        """)

    cur.execute(query)

    # Obtém os resultados
    rows = cur.fetchall()

    # Fecha o cursor e a conexão
    cur.close()
    conn.close()

    # Para pegar o nome das colunas, você pode usar cur.description
    columns = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=columns)
    
    # Concatenando dimensão de cnae e natureza juridica
    df = df.merge(aux_cnae, on = ['cod_cnae'], how = 'inner')
    df = df.merge(aux_nat_ju, on = ['cod_natureza_juridica'], how = 'inner')
    
    # criando função para verificar se é SPE, Consorcio ou Construtora
    def spe_consorcio_construtora(data_frame):
        is_spe = (data_frame['cod_natureza_juridica'] == '2062') & (data_frame['razao_social'].str.startswith("SPE ") | data_frame['razao_social'].str.endswith(" SPE"))

        is_consorcio = (data_frame['cod_natureza_juridica'].isin(['1210', '1228', '2151', '2283', '2291']) & (data_frame['razao_social'].str.startswith("CONSORCIO ") | data_frame['razao_social'].str.endswith(" CONSORCIO")))

        is_construtora = (data_frame['cod_natureza_juridica'].isin(['2062', '2135']) & data_frame['cod_cnae'].isin(['3011301', '3011302', '3012100', '4120400', '4211101', '4212000', '4221901', '4221902', '4221904', '4222701', '4223500', '4299501']))

        return (is_spe | is_consorcio | is_construtora)

    df['is_spe_consorcio_construtora'] = spe_consorcio_construtora(df)
    
    # VERIFICANDO EM QUE RAMIFICAÇÃO O CNPJ CAI
    df['ramificacao_pre_filtro'] = None

    # PF 1
    df.loc[df['situacao_cadastral'] != 'ATIVA', 'ramificacao_pre_filtro'] = 'PF 1'
    # PF 2
    df.loc[(df['situacao_especial'] == 'RECUPERACAO JUDICIAL') & (df['ramificacao_pre_filtro'].isna()), 'ramificacao_pre_filtro'] = 'PF 2'
    # PF 3
    df.loc[((df['is_mei'] == True) | (df['is_mei'] == 'True')) & (df['ramificacao_pre_filtro'].isna()), 'ramificacao_pre_filtro'] = 'PF 3'
    # PF 4
    df.loc[(df['cnae_aceito'] == 'NAO') & (df['ramificacao_pre_filtro'].isna()), 'ramificacao_pre_filtro'] = 'PF 4'
    # PF 5
    df.loc[(df['nat_ju_aceita'] == 'NAO') & (df['ramificacao_pre_filtro'].isna()), 'ramificacao_pre_filtro'] = 'PF 5'
    # PF 6
    df.loc[(df['idade'] < 2) & (df['ramificacao_pre_filtro'].isna()), 'ramificacao_pre_filtro'] == 'PF 6'
    # PF 7
    df.loc[((df['tem_pep'] == 'True') | (df['tem_pep'] == True)) & (df['ramificacao_pre_filtro'].isna()), 'ramificacao_pre_filtro'] = 'PF 7'
    # PF 8
    df.loc[(df['idade_socio'].notna()) & ((df['idade_socio'] < 2) | ((df['tem_socio_pj'] == 'True') | (df['tem_socio_pj'] == True))) & (df['ramificacao_pre_filtro'].isna()), 'ramificacao_pre_filtro'] = 'PF 8'
    # PF 9
    df.loc[(df['is_spe_consorcio_construtora']) & (df['ramificacao_pre_filtro'].isna()), 'ramificacao_pre_filtro'] = 'PF 9'
    # PF 10
    df.loc[(df['inad_alpe'] == 'SIM') & (df['ramificacao_pre_filtro'].isna()), 'ramificacao_pre_filtro'] = 'PF 10'
    # PF 11
    #df.loc[((df['limite_alpe'] == True) | (df['limite_alpe'] == 'True')) & (df['ramificacao_pre_filtro'].isna()), 'ramificacao_pre_filtro'] = 'PF 11'
    # PF 12
    df.loc[df['ramificacao_pre_filtro'].isna(), 'ramificacao_pre_filtro'] = 'PF 12'
 
 
     #TRATANDO A RESPOSTA COM BASE NA RAMIFICAÇÃO   
    df['resposta'] = None

    # REPROVADO
    df.loc[df['ramificacao_pre_filtro'].isin(['PF 1', 'PF 2', 'PF 3', 'PF 4', 'PF 5', 'PF 6', 'PF 7', 'PF 10']), 'resposta'] = 'REPROVADO'

    # MESA
    df.loc[df['ramificacao_pre_filtro'].isin(['PF 8', 'PF 9', 'PF 11']), 'resposta'] = 'MESA'

    # SEGUE
    df.loc[df['ramificacao_pre_filtro'].isin(['PF 12']), 'resposta'] = 'SEGUE'
    
    # Nome do arquivo CSV que você deseja criar
    file_out = f'LANDING_PRE_FILTRO.csv'
    
    csv_bytes = df.to_csv(index=False, sep=';').encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)

    client.put_object(f'{BUCKET_SOURCE_REFINED}',
                        f'{FOLDER_DESTINATION_REFINED}/{file_out}',
                            data=csv_buffer,
                            length=len(csv_bytes))
