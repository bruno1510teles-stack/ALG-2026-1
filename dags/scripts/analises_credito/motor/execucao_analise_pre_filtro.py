# Carregando libs
import pandas as pd
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import os, pytz
from datetime import datetime


def analise_pre_filtro(access_params=None):

    # VARIAVEIS DE DOS ARQUIVOS
    fuso_horario = pytz.timezone('America/Sao_Paulo')
    agora = datetime.now(fuso_horario)
    ano = agora.strftime('%Y')
    mes = agora.strftime('%m')
    dia = agora.strftime('%d')
    hora = agora.strftime('%H')

    BUCKET_SOURCE_REFINED = "motor"
    FOLDER_SOURCE_REFINED = 'analise_credito/in/a_processar'
    FOLDER_DESTINATION_REFINED = 'analise_credito/auxiliar'

    # Conectando na refined
    client = Minio(
        access_params['endpoint_url_refined'],
        access_key=access_params['aws_access_key_id_refined'],
        secret_key=access_params['aws_secret_access_key_refined'],
    )


    # BAIXANDO ARQUIVO A SER ANALISADO
    file = client.get_object(
        bucket_name=BUCKET_SOURCE_REFINED, 
        object_name=f'{FOLDER_DESTINATION_REFINED}/LANDING_BASE_ANALISAR.csv'
    )

    base_analisar = pd.read_csv(BytesIO(file.data), dtype=str, sep=';')
    base_analisar['CNPJ'] = base_analisar['CNPJ'].str.zfill(14)
    base_analisar['cnpj_raiz'] = base_analisar['CNPJ'].str.slice(0, 8).str.zfill(8)
    print(f"Quantidade de CNPJs na base_analisar: {base_analisar.shape[0]}")

    cnpjs = base_analisar['CNPJ'].unique()
    cnpjs_raiz = base_analisar['cnpj_raiz'].str.slice(0, 8).unique()
    print(f"Quantidade de CNPJs na cnpjs_raiz: {cnpjs_raiz.shape[0]}")

    ## Criar uma string formatada para a cláusula IN
    ids_query = ', '.join(f"'{cnpj}'" for cnpj in cnpjs_raiz)
    print(ids_query)
    ids_query = f"({ids_query})"
    
    conn = connect(
        host=access_params['trino_endpoint'],
        port=access_params['trino_port'],
        user=access_params['trino_user'],
        auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
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
        distinct est.documento_sem_formatacao
    from 
        deltalaketrusted.receita_federal.estabelecimentos est
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
    print(f"Quantidade de CNPJs que retornou da estabelecimentos: {base_analisar_raiz.shape[0]}")

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
            left join deltalaketrusted.receita_federal.empresas emp on emp.cnpj_raiz = pre.cnpj_raiz --and pre.data_ref_receita = emp.data_ref
            left join limite lim on lim.cnpj_raiz = pre.cnpj_raiz 
            left join deltalaketrusted.receita_federal.estabelecimentos est on est.documento_sem_formatacao = pre.documento_sem_formatacao
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
    df['todos_cnaes'] = df.apply(lambda row: [row['cod_cnae']] + row['cnae_secundaria'].split(','), axis=1)
    # Passo 2: Explodir o DataFrame para que cada CNAE fique em uma linha separada
    df_exploded = df.explode('todos_cnaes')
    # Passo 3: Fazer o merge para validar os CNAEs
    df_validado = df_exploded.merge(aux_cnae, on = ['cod_cnae'], how = 'inner')
    # Passo 4: Consolidar o resultado para manter uma linha por CNPJ e verificar se ao menos um CNAE foi aceito
    df = df_validado.groupby('documento_sem_formatacao').agg({
        'cod_cnae': 'first',  # Mantém o CNAE principal
        'todos_cnaes': lambda x: ','.join(x),  # Junta os CNAEs novamente
        'cnae_aceito': lambda x: 'SIM' if 'SIM' in x.values else 'NAO',  # Se qualquer um for 'SIM', aceita
    })
    

    print(f"DF pós tratamento cnaes secundário: {df}")

    ### Concatenando base principal(import)
    df = df.merge(base_analisar[['cnpj_raiz', 'issue_jira', 'inad_alpe', 'pgid']], on = ['cnpj_raiz'], how = 'left')
    
    # Concatenando dimensão natureza juridica
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
    # PF 7
    df.loc[((df['tem_pep'] == 'True') | (df['tem_pep'] == True)) & (df['ramificacao_pre_filtro'].isna()), 'ramificacao_pre_filtro'] = 'PF 7'
    # PF 11
    df.loc[((df['limite_alpe'] == True) | (df['limite_alpe'] == 'True')) & (df['ramificacao_pre_filtro'].isna()), 'ramificacao_pre_filtro'] = 'PF 11'
    # PF 3
    df.loc[((df['is_mei'] == True) | (df['is_mei'] == 'True')) & (df['ramificacao_pre_filtro'].isna()), 'ramificacao_pre_filtro'] = 'PF 3'
    # PF 4
    df.loc[(df['cnae_aceito'] == 'NAO') & (df['ramificacao_pre_filtro'].isna()), 'ramificacao_pre_filtro'] = 'PF 4'
    # PF 5
    df.loc[(df['nat_ju_aceita'] == 'NAO') & (df['ramificacao_pre_filtro'].isna()), 'ramificacao_pre_filtro'] = 'PF 5'
    # PF 6
    df.loc[(df['is_spe_consorcio_construtora']) & (df['ramificacao_pre_filtro'].isna()), 'ramificacao_pre_filtro'] = 'PF 6'
    # PF 8
    df.loc[(((df['idade_socio'].notna()) & (df['idade_socio'] < 2)) | (df['tem_socio_pj'] == True)) & (df['ramificacao_pre_filtro'].isna()), 'ramificacao_pre_filtro'] = 'PF 8'
    # PF 9
    df.loc[((df['idade'] < 2) & (df['ramificacao_pre_filtro'].isna())), 'ramificacao_pre_filtro'] = 'PF 9'
    # PF 10
    df.loc[(df['inad_alpe'] == 'SIM') & (df['ramificacao_pre_filtro'].isna()), 'ramificacao_pre_filtro'] = 'PF 10'
    # PF 12
    df.loc[df['ramificacao_pre_filtro'].isna(), 'ramificacao_pre_filtro'] = 'PF 12'
 
     #TRATANDO A RESPOSTA COM BASE NA RAMIFICAÇÃO   
    df['resposta'] = None

    # REPROVADO
    df.loc[df['ramificacao_pre_filtro'].isin(['PF 1', 'PF 2', 'PF 3', 'PF 4', 'PF 5', 'PF 7', 'PF 9', 'PF 10']), 'resposta'] = 'REPROVADO'

    # MESA
    df.loc[df['ramificacao_pre_filtro'].isin(['PF 6', 'PF 8', 'PF 11']), 'resposta'] = 'MESA'

    # SEGUE
    df.loc[df['ramificacao_pre_filtro'].isin(['PF 12']), 'resposta'] = 'SEGUE'

    df['politica'] = 'Arcelor Mittal'
    df['versao_motor'] = '1.1'
    
    # Nome do arquivo CSV que você deseja criar
    file_out = f'LANDING_PRE_FILTRO.csv'
    
    print(df)


    csv_bytes = df.to_csv(index=False, sep=';').encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)

    client.put_object(f'{BUCKET_SOURCE_REFINED}',
                        f'{FOLDER_DESTINATION_REFINED}/{file_out}',
                            data=csv_buffer,
                            length=len(csv_bytes))
