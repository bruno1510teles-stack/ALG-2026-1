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


def analise_pre_filtro_v5(access_params=None,  **kwargs):

    # Conectando com Minio para pegar input da base
    minio_raw = Minio(
        "api-raw.alpe.com.br",
        access_key = 'B7q0avvSIpSdyGPXWnEC',
        secret_key = 'PhMhRQSQ6YJU8fn2qKhDLM017cQPrlCz1YbM8IwU'
    )


    # Connection validation
    try:
        # Try to list the buckets
        buckets = minio_raw.list_buckets()
        
        # If the connection was successful, print the buckests
        print("Conexão bem-sucedida. Lista de buckets disponíveis:")
        for bucket in buckets:
            print(bucket.name)
        
    except Exception as e:
            # If the connection was failed, print the error message
            print(f"Erro ao conectar ao MinIO: {e}")


    # Bucket and Folder_Destination
    BUCKET_SOURCE_RAW = "pre-aprovado-lote"
    FOLDER_DESTINATION_RAW = 'year=2025/month=1/day=6'
    file_name = 'Base de Clientes Matcon - CDB Curitiba.xlsx'
    file_path = f'{FOLDER_DESTINATION_RAW}/{file_name}'


    # Uploading Excel File
    response = minio_raw.get_object(BUCKET_SOURCE_RAW, file_path)
    file_data = BytesIO(response.read())
    base_analisar = pd.read_excel(file_data)


    # Tratando base
    base_analisar['CNPJ'] = base_analisar['CNPJ'].astype(str).str.zfill(14)
    base_analisar['cnpj_raiz'] = base_analisar['CNPJ'].str.slice(0, 8).str.zfill(8)
    print(f"Quantidade de CNPJs na base_analisar: {base_analisar.shape[0]}")


    cnpjs = base_analisar['CNPJ'].unique()
    cnpjs_raiz = base_analisar['cnpj_raiz'].str.slice(0, 8).unique()
    print(f"Quantidade de CNPJs na cnpjs_raiz: {cnpjs_raiz.shape[0]}")


    # Criar uma string formatada para a cláusula IN
    ids_query = ', '.join(f"'{cnpj}'" for cnpj in cnpjs_raiz)
    # print(ids_query)
    ids_query = f"({ids_query})"

    if ids_query == "()":
        ids_query = "('')"


        ### Validando se a raiz do CNPJ foi analisada a menos de 60 DIAS
    # Configurações da API do Jira
    jira_url = "https://alpe.atlassian.net/rest/api/2/search"
    # Credenciais de acesso
    email = "felipe.ferraz@alpe.com.br"
    api_token = "ATATT3xFfGF0HVdx6POVBSFWH3BnpC0HyKvTF9EXgjLZ6rpwZuHnIAcI1UDeNTht79mL-O60ezlE4a2qKr4d-H_A3DmY7VCwATPRMABBMQns1ubEjMI_uFgCjEMPeUKkpVgb_-BZ5btV7yQQal1ZNmEm2dGZY_NTpUpOkHdC-SjK0iiBIj3EP80=037ADAA4"
    # Gerando o header de autenticação em Base64
    auth = base64.b64encode(f"{email}:{api_token}".encode()).decode()

    headers = {
    "Authorization": f"Basic {auth}",
    "Content-Type": "application/json"
    }   

    max_results = 100  # Defina o número máximo de resultados por página (até 1000 conforme a configuração do Jira)
    start_at = 0       # Inicie na primeira página de resultados
    all_tickets = []   # Lista para armazenar todos os tickets
    data = []   # Data para armazenar os resultados finais

    # Iterando sobre cada cnpj_raiz
    for cnpj in cnpjs_raiz:
        start_at = 0  # Resetando o início da paginação para cada CNPJ
        has_tickets = False  # Flag para verificar se algum ticket foi retornado

        while True:
            # Query para buscar os tickets da fila desejada, alterando o CNPJ em cada iteração
            query = {
                "jql": f'project = CMGT AND "Payer Identification[Short text]" ~ "{cnpj}*" AND created >= -60d',
                "fields": ["key",  # ISSUE_JIRA
                        "customfield_13808",  # LIMITE ALPE
                        "assignee",
                        "customfield_13729"
                        "resolution"],  # Adicionando o assignee corretamente
                "maxResults": max_results,
                "startAt": start_at
            }

            # Fazendo a requisição para o Jira
            response = requests.get(jira_url, headers=headers, params=query)

            if response.status_code == 200:
                # Processando a resposta
                tickets = response.json().get('issues', [])
                
                if not tickets:
                    # Se não houver mais tickets, parar a paginação
                    break

                # Se houver tickets, marcamos que foi encontrado algo
                has_tickets = True
                for ticket in tickets:
                    # Acessando o assignee corretamente dentro de fields
                    assignee = ticket['fields'].get('assignee')
                    #cpnj_jira = ticket['fields'].get('customfield_13729')
                    assignee_name = assignee['displayName'] if assignee else 'Não atribuído'
                    resolucao = ticket['fields'].get('resolution', {})
                    if resolucao is None:
                        decisao = 'NF'
                    else:
                        decisao = resolucao.get('name', 'NF')

                    # Adiciona ticket à lista all_tickets
                    all_tickets.append(ticket)

                    # Adiciona dados ao DataFrame
                    data.append({
                        'cnpj_raiz': cnpj,
                        #'cnpj_jira': cpnj_jira,
                        'analise_menor_60_dias': True,
                        'decisor': assignee_name,
                        'decisao': decisao
                    })
                
                # Atualiza o ponto inicial para a próxima página
                start_at += max_results
            else:
                print(f"Failed to fetch data from Jira for CNPJ {cnpj}: {response.status_code}")
                break

        # Se nenhum ticket foi encontrado, adicionar uma linha indicando isso
        if not has_tickets:
            data.append({
                'cnpj_raiz': cnpj,
                #'cnpj_jira': cpnj_jira,
                'analise_menor_60_dias': False,
                'decisor': None,  # Nenhum decisor para esse CNPJ
                'decisao': None  # Nenhum limite encontrado
            })

    # Criando um DataFrame com os resultados
    df_jira = pd.DataFrame(data)
    df_jira = df_jira[df_jira['decisao'] != 'Duplicado']
    df_jira = df_jira.drop_duplicates(subset='cnpj_raiz')
    df_jira['decisor'] = np.where(df_jira['decisor'] == 'Jira Service User', 'Motor', 'Mesa')


    # Conectando com o banco de dados Trino
    conn = connect(
        host='trino.alpe.com.br',
        port='443',
        user='trinodados',
        auth=BasicAuthentication('trinodados', 'hosgzPvuhyXkP<j}RyT+'),
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

    if base_analisar_raiz['cnpj_raiz'].isnull().all() or base_analisar_raiz['cnpj_raiz'].empty:
    # Se estiver vazio, usa cnpj_raiz da base inicial
        base_analisar_raiz['cnpj_raiz'] = base_analisar['cnpj_raiz'].str.slice(0, 8).str.zfill(8)

    df = pd.DataFrame()

    # Cria um cursor e executa a query
    cur = conn.cursor()

    aux = 0

    for index, row in base_analisar_raiz.iterrows():
        cnpj_completo = row['documento_sem_formatacao']
        cnpj_raiz = row['cnpj_raiz'] 

        # Marca o tempo de início
        start_time = time.time()

        query = (f"""

            with limite as (
                select 
                    cnpj_raiz, case when limite_atribuido > 0 then true else false end as limite_alpe, NULLIF(limite_disponivel, 0) / NULLIF(limite_atribuido, 0) AS pcto_limite_utilizado, situacao_sacado, limite_atribuido
                from (
                    select 
                        pc.chave as cnpj_raiz,
                        lc.id is not null as limite_alpe,
                        sum(limite_atribuido) as limite_atribuido,
                        sum(limite_disponivel) as limite_disponivel,
                        pl.status as situacao_sacado
                    from postgres.ccred_schema_prd_default.participante_chave pc
                    inner join postgres.ccred_schema_prd_default.limite_config lc on lc.participante_chave_sacado_id = pc.id
                    inner join postgres.ccred_schema_prd_default.participante_limite pl on pl.limite_config_id = lc.id	    
                    where
                    pc.chave = COALESCE('{cnpj_raiz}', '')
                    group by
                        pc.chave, lc.id is not null, pl.status)
                            )                    
            select 
                pre.cnpj_raiz cnpj_raiz,
                pre.documento_sem_formatacao,
                pre.razao_social,
                pre.cod_cnae,
                pre.cnae_secundaria, 
                pre.cod_natureza_juridica,
                CAST(pre.codigo_porte_empresa AS DECIMAL) AS codigo_porte_empresa,
                pre.capital_social_empresa as "Capital Social",
                pre.idade,
                pre.situacao_cadastral situacao_cadastral,
                pre.idade_socio,
                pre.tem_socio_pj,
                pre.is_mei,
                pre.tem_pep,
                pre.situacao_especial,
                pre.data_ref_receita,
                coalesce(lim.limite_alpe, false) as limite_alpe,
                coalesce(lim.situacao_sacado, 'ATIVO') as situacao_sacado,
                lim.pcto_limite_utilizado,
                coalesce(lim.limite_atribuido, 0) as limite_atribuido
            from 
                deltalakerefined.motor.pre_filtro pre
            left join limite lim on lim.cnpj_raiz = pre.cnpj_raiz
            where pre.documento_sem_formatacao =  COALESCE('{cnpj_completo}', '')


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

        # Marca o tempo de fim e calcula a duração
        end_time = time.time()
        elapsed_time = end_time - start_time

        # Exibe o tempo de execução para cada CNPJ
        print(f"Tempo de execução para CNPJ {cnpj_completo} ({aux}): {elapsed_time:.2f} segundos")

        aux = aux + 1

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
    df_validado = df_exploded.merge(aux_cnae, left_on='todos_cnaes', right_on='cod_cnae', how='left')
    # Passo 4: Consolidar o resultado para manter uma linha por CNPJ e verificar se ao menos um CNAE foi aceito
    df = df_validado.groupby('documento_sem_formatacao').agg({
        'cnpj_raiz': 'first',  # Mantém o primeiro valor da coluna cnpj_raiz (assumindo que seja o mesmo para cada grupo)
        'cod_cnae_x': 'first',  # Mantém o CNAE principal
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
        'limite_alpe': 'first',  # Mantém o primeiro valor da coluna limite_alpe
        'situacao_sacado': 'first',
        'pcto_limite_utilizado': 'first',
        'limite_atribuido' : 'first'
    }).reset_index()  # Reseta o índice para retornar um DataFrame regular

    df.rename(columns={'cod_cnae_x': 'cod_cnae'}, inplace=True)


    ### Concatenando base principal(import)
    df = df.merge(base_analisar, on = ['cnpj_raiz'], how = 'outer')
    df = df.merge(df_jira[['cnpj_raiz', 'analise_menor_60_dias', 'decisor', 'decisao']], on = ['cnpj_raiz'], how = 'left')

    df = df.drop(columns=['documento_sem_formatacao'])
    df.rename(columns={'CNPJ': 'documento_sem_formatacao'}, inplace=True)


    pd.set_option('display.max_rows', None)  # Mostra todas as linhas
    pd.set_option('display.max_columns', None)  # Mostra todas as colunas
    pd.set_option('display.width', None)  # Ajusta a largura para que o DataFrame não quebre em várias linhas
    pd.set_option('display.max_colwidth', None)  # Permite exibir o conteúdo completo de cada coluna


    ### Cruzando DF
    df = df.merge(aux_nat_ju, on = ['cod_natureza_juridica'], how = 'left')


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
        # Impedidos de Operar
        (df['idade'].isna() , 'PF FUNDACAO < 2 ANOS'),
        (df['situacao_cadastral'] != 'ATIVA', 'PF CNPJ IRREGULAR'),

        # Filtro Operacional
        (df['situacao_sacado'] != 'ATIVO', 'PF BLOQUEIO ALPE'),
        ((df['analise_menor_60_dias'] == True) & (df['decisao'] == 'Reproved'), 'PF REPROVA < 60 DIAS'),
        #((df['analise_menor_60_dias'] == True) & (df['decisao'] == 'Approved') & ((df['limite_solicitado']*1.2)  <= df['limite_atribuido']) & pd.notna(df['limite_atribuido']),
        # 'PF LIMITE SOLICITADO <= ATUAL'),
        #((df['analise_menor_60_dias'] == True) & (df['decisao'] == 'Approved') & (df['limite_solicitado'] * 1.2 > df['limite_atribuido']) & (df['pcto_limite_utilizado'] < 0.7) & pd.notna(df['pcto_limite_utilizado']),
        #'PF - UTILIZAÇÃO DE LIMITE MÍNIMA NÃO ATINGIDA'),

        # Filtro de Política
        (df['situacao_especial'] == 'RECUPERACAO JUDICIAL', 'PF RJ'),
        ((df['tem_pep'] == 'True') | (df['tem_pep'] == True), 'PF PEP'),
        ((df['limite_alpe'] == True) | (df['limite_alpe'] == 'True'), 'PF MESA'),
        ((df['is_mei'] == True) | (df['is_mei'] == 'True'), 'PF MEI'),
        (df['cnae_aceito'] == 'NAO', 'PF CNAE'),
        (df['nat_ju_aceita'] == 'NAO', 'PF NATUREZA JURIDICA'),
        (df['is_spe_consorcio_construtora'], 'PF CONSORCIO/CONSTRUTORA/SPE'),
        (((df['idade_socio'].notna()) & (df['idade_socio'] < 2)) | (df['tem_socio_pj'] == True), 'PF SOCIO PJ OU < 2 ANOS'),
        (df['idade'] < 2, 'PF FUNDACAO < 2 ANOS')
    ]
    # Aplicar condições
    for condition, value in conditions:
        df.loc[condition & df['ramificacao_pre_filtro'].isna(), 'ramificacao_pre_filtro'] = value

    # Atribuir 'PF 12' para os que não se encaixam em nenhuma das condições anteriores
    df.loc[df['ramificacao_pre_filtro'].isna(), 'ramificacao_pre_filtro'] = 'PF SEGUE'


    # Criando Resposta
    response_map = {
        'REPROVADO': ['PF CNPJ IRREGULAR', 'PF REPROVA < 60 DIAS', 'PF RJ', 'PF PEP', 'PF MEI', 'PF CNAE', 'PF NATUREZA JURIDICA', 'PF FUNDACAO < 2 ANOS'],
        'mantido' : ['PF LIMITE SOLICITADO <= ATUAL', 'PF - UTILIZAÇÃO DE LIMITE MÍNIMA NÃO ATINGIDA'],
        'MESA': ['PF MESA', 'PF CONSORCIO/CONSTRUTORA/SPE', 'PF SOCIO PJ OU < 2 ANOS', 'PF BLOQUEIO ALPE'],
        'SEGUE': ['PF SEGUE']
    }
    # Aplicar as respostas
    for response, values in response_map.items():
        df.loc[df['ramificacao_pre_filtro'].isin(values), 'resposta'] = response

    # Definindo a Política e Versão
    df['politica'] = 'V5'
    df['versao_motor'] = '1.0'


    # Contar a quantidade por ramificação
    qtd_por_ramificacao = df.groupby('resposta').size()

    # Printando o resultado
    print(qtd_por_ramificacao)


    pd.reset_option('display.max_rows')
    pd.reset_option('display.max_columns')
    pd.reset_option('display.width')
    pd.reset_option('display.max_colwidth')

    ### Salvando DF para utilizar na próxima tarefa da DAG
    return df.to_dict(orient='records')