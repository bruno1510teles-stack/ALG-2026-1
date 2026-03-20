# Importing Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import numpy as np
from airflow.models import Variable




def extracao_faturamento_externo(access_params=None, **kwargs):


    # CARREGANDO BASE FOTO, OU SEJA MINHA BASE ATUAL
    conn = connect(
        host=Variable.get("TRINO_ENDPOINT"),
        port=Variable.get("TRINO_PORT"),
        user=Variable.get("TRINO_USER"),
        auth=BasicAuthentication(Variable.get("TRINO_USER"), Variable.get("TRINO_PASSWORD")),
        http_scheme="https",
    )


    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        
        return pd.DataFrame(rows, columns=columns)

    query_base_fat_externo_trusted = f"""
                                        select *
                                        from deltalaketrusted.payments.faturamento_externo_arcelor_aux
                                    """

    df = execute_query(conn, query_base_fat_externo_trusted)

    print('DADOS AUX CARREGADOS DO TRINO COM SUCESSO!')


    # IMPORTA BASE QUE VAI SER ACUMULADA
    minio_raw = Minio(
        Variable.get("MINIO_RAW_ENDPOINT"),
        access_key=Variable.get("MINIO_RAW_ACCESS_KEY"),
        secret_key=Variable.get("MINIO_RAW_SECRET_KEY"),
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

    print('Importando Bases...')

    # BASE1
    # Bucket and Folder_Destination
    BUCKET_SOURCE_RAW = "faturamento-externo"
    FOLDER_DESTINATION_RAW = 'arcelor/year=2026/month=03/day=19'
    file_name = 'Faturamento Base Jan-Nov25.xlsx'
    file_path = f'{FOLDER_DESTINATION_RAW}/{file_name}'

    # Uploading Excel File
    response = minio_raw.get_object(BUCKET_SOURCE_RAW, file_path)
    file_data = BytesIO(response.read())
    base1 = pd.read_excel(file_data, sheet_name="Planilha1", header=1)



    # TRATANDO BASE NOVA

    # Treating column names
    def format_column_names(df):
        new_columns = []
        for col in df.columns:
            try:
                new_col = pd.to_datetime(col).strftime('%Y%m')
                new_columns.append(new_col)
            except (ValueError, TypeError):
                new_columns.append(col)
            
        df.columns = new_columns

    format_column_names(base1)

    #base1 = base1.dropna(subset=['Razão Social']).copy()

    # Adjusting columns with upper()
    #base1['Razão Social'] = base1['Razão Social'].str.upper()
    base1['Unidade'] = base1['Unidade'].str.upper()


    def tratar_colunas_202(df):
        # Filtrando colunas que começam com "202"
        colunas_202 = df.filter(like='202').columns

        # Para colunas numéricas: substituir valores negativos por 0
        for col in colunas_202:
            if df[col].dtype in ['int64', 'float64']:
                df[col] = df[col].clip(lower=0)  # Substitui valores negativos por 0
        
        # Para colunas de string: transformar todos os textos em maiúsculas
        for col in colunas_202:
            if df[col].dtype == 'object':  # Verifica se a coluna é de string (object)
                df[col] = pd.to_numeric(df[col].str.replace(' BRL', '').str.replace('.', '').str.replace(',', '.'), errors='coerce')
        
        return df

    tratar_colunas_202(base1)


    # Treating NaN values
    def fillna_in_columns_starting_with(df, prefix, value):
        # Filtering columns
        filtered_columns = [col for col in df.columns if str(col).startswith(prefix)]
        
        df[filtered_columns] = df[filtered_columns].fillna(value)

    fillna_in_columns_starting_with(base1, '20', 0)


    #Transforming float64 in float
    base1[base1.select_dtypes(include=['float64']).columns] = base1.select_dtypes(include=['float64']).astype(float)

    #Transforming ['Raiz CNPJ']
    base1['Raiz CNPJ'] = '00000000' + base1['Raiz CNPJ'].astype(str)
    base1['Raiz CNPJ'] = base1['Raiz CNPJ'].str[-8:]

    # Removendo coluna antiga de Raiz CNPJ antes do tratamento
    #base1 = base1.drop(columns=["Pagador"])

    # Renomeando colunas
    #base1.rename(columns={'Raiz CNPJ': 'raiz_cnpj', 'Razão Social': 'razao_social', 'Unidade':'unidade'}, inplace=True)
    base1.rename(columns={'Raiz CNPJ': 'raiz_cnpj', 'Unidade':'unidade'}, inplace=True)


    # DEPARA UNIDADE CONSOLIDADA PARA CRUZAMENTO
    # Bucket and Folder_Destination
    BUCKET_SOURCE_RAW_2 = "arquivos-python"
    FOLDER_DESTINATION_RAW_2 = 'depara_unidade_fat_externo'
    file_name_2 = 'depara_unidade_fat_externo.xlsx'
    file_path_2 = f'{FOLDER_DESTINATION_RAW_2}/{file_name_2}'

    # Uploading Excel File
    response_2 = minio_raw.get_object(BUCKET_SOURCE_RAW_2, file_path_2)
    file_data_2 = BytesIO(response_2.read())
    df_unidade_consolidada = pd.read_excel(file_data_2, sheet_name="unidade_consolidada")

    # BASE 1
    base1 = pd.merge(base1, df_unidade_consolidada, on = 'unidade', how='left')
    base1['unidade_consolidada'] = base1['unidade_consolidada'].fillna(base1['unidade'])


    # Dropar a colunas antigas
    #base1 = base1.drop(columns=['unidade', 'razao_social'])
    base1 = base1.drop(columns=['unidade'])
    base1 = base1.drop_duplicates()



    def cruzar_bases(base1, base2, chave=['raiz_cnpj', 'unidade_consolidada']):

        # Identificar colunas exclusivas da base2
        colunas_novas = [col for col in base2.columns if col not in base1.columns]

        # Identificar as colunas comuns entre base1 e base2, excluindo as colunas a serem ignoradas
        colunas_iguais = [col for col in base2.columns if col in base1.columns and col not in chave]

        # Garantir que base1 tenha apenas um valor por chave, priorizando soma maior das colunas
        base1 = base1.assign(soma_colunas_novas=base1[colunas_iguais].sum(axis=1))
        base1 = base1.sort_values(by='soma_colunas_novas', ascending=False).drop_duplicates(subset=chave).drop(columns='soma_colunas_novas')

        # Garantir que base2 tenha apenas um valor por chave, priorizando soma maior nas colunas_novas
        base2 = base2.assign(soma_colunas_novas=base2[colunas_novas].sum(axis=1))
        base2 = base2.sort_values(by='soma_colunas_novas', ascending=False).drop_duplicates(subset=chave).drop(columns='soma_colunas_novas')

        # Casos que aparecem nas duas bases
        base_comum = pd.merge(base1, base2[chave + colunas_novas], on=chave, how='inner')

        # Casos que aparecem apenas na base1
        apenas_base1 = base1[~base1[chave].apply(tuple, axis=1).isin(base2[chave].apply(tuple, axis=1))]

        # Casos que aparecem apenas na base2
        apenas_base2 = base2[~base2[chave].apply(tuple, axis=1).isin(base1[chave].apply(tuple, axis=1))]

        # Concatenar os dados para formar a base final
        base_final = pd.concat([base_comum, apenas_base1, apenas_base2], ignore_index=True)

        # Preencher valores ausentes com 0
        base_final = base_final.fillna(0)

        # Removendo linhas iguais
        base_final = base_final.drop_duplicates()

        return base_final
    

    # DROPAR COLUNAS DA BASE FOTO ANTES DO MERGE
    df = df.drop(columns=['razao_social', 'cidade', 'uf', 'atualizado_em', 'year', 'month', 'day', 'cnae_principal'])


    base_final_merge = cruzar_bases(df, base1)


    # Inserindo Cidade, UF e Cnae Principal

    # Extraindo os CNPJs do DataFrame 'fat_pag' e convertendo-os para uma lista
    cnpjs = base_final_merge['raiz_cnpj'].unique().tolist()

    tamanho = len(cnpjs) // 4

    cnpj_part_1 = cnpjs[:tamanho]
    cnpj_part_2 = cnpjs[tamanho:2*tamanho]
    cnpj_part_3 = cnpjs[2*tamanho:3*tamanho]
    cnpj_part_4 = cnpjs[3*tamanho:]

    # Convertendo a lista para uma string no formato adequado para o SQL
    cnpjs_str_1 = ', '.join([f"'{cnpj}'" for cnpj in cnpj_part_1])
    cnpjs_str_2 = ', '.join([f"'{cnpj}'" for cnpj in cnpj_part_2])
    cnpjs_str_3 = ', '.join([f"'{cnpj}'" for cnpj in cnpj_part_3])
    cnpjs_str_4 = ', '.join([f"'{cnpj}'" for cnpj in cnpj_part_4])

    print('Carregando dados de Cidade e Cnae....')

    query_receita_1 = f"""
                        select 	distinct
                                cnpj_raiz as raiz_cnpj,
                                municipio as cidade,
                                uf,
                                cast(cnae_principal_codigo as varchar) as cnae_principal
                        from deltalakerefined.receita_federal.dados_cadastrais
                        where flag_matriz = 'Sim'
                        and cnpj_raiz in ({cnpjs_str_1})
                    """

    query_receita_2 = f"""
                        select 	distinct
                                cnpj_raiz as raiz_cnpj,
                                municipio as cidade,
                                uf,
                                cast(cnae_principal_codigo as varchar) as cnae_principal
                        from deltalakerefined.receita_federal.dados_cadastrais
                        where flag_matriz = 'Sim'
                        and cnpj_raiz in ({cnpjs_str_2})
                    """

    query_receita_3 = f"""
                        select 	distinct
                                cnpj_raiz as raiz_cnpj,
                                municipio as cidade,
                                uf,
                                cast(cnae_principal_codigo as varchar) as cnae_principal
                        from deltalakerefined.receita_federal.dados_cadastrais
                        where flag_matriz = 'Sim'
                        and cnpj_raiz in ({cnpjs_str_3})
                    """

    query_receita_4 = f"""
                        select  distinct
                                cnpj_raiz as raiz_cnpj,
                                municipio as cidade,
                                uf,
                                cast(cnae_principal_codigo as varchar) as cnae_principal
                        from deltalakerefined.receita_federal.dados_cadastrais
                        where flag_matriz = 'Sim'
                        and cnpj_raiz in ({cnpjs_str_4})
                    """

    receita_1 = execute_query(conn, query_receita_1)
    receita_2 = execute_query(conn, query_receita_2)
    receita_3 = execute_query(conn, query_receita_3)
    receita_4 = execute_query(conn, query_receita_4)

    receita = pd.concat([receita_1, receita_2, receita_3, receita_4], ignore_index=True)

    print(receita)

    # Fazendo JOIN

    receita['raiz_cnpj'] = '00000000' + receita['raiz_cnpj'].astype(str)
    receita['raiz_cnpj'] = receita['raiz_cnpj'].str[-8:]

    df_final = pd.merge(base_final_merge, receita, on='raiz_cnpj', how='left')

    df_final = df_final.reset_index(drop=True)

    print('Após JOIN com a Receita:')
    print(df_final)

    # INSERINDO RAZAO SOCIAL DO NOSSO BANCO DE DADOS

    print('Carregando dados de Razão Social...')

    query_empresas_1 =  f""" 
                        select  distinct
                                cnpj_raiz as raiz_cnpj,
                                razao_social
                        from deltalaketrusted.receita_federal.empresas
                        where cnpj_raiz in ({cnpjs_str_1})
                    """


    query_empresas_2 =  f""" 
                        select  distinct
                                cnpj_raiz as raiz_cnpj,
                                razao_social
                        from deltalaketrusted.receita_federal.empresas
                        where cnpj_raiz in ({cnpjs_str_2})
                    """

    query_empresas_3 =  f""" 
                        select  distinct
                                cnpj_raiz as raiz_cnpj,
                                razao_social
                        from deltalaketrusted.receita_federal.empresas
                        where cnpj_raiz in ({cnpjs_str_3})
                    """

    query_empresas_4 =  f""" 
                        select  distinct
                                cnpj_raiz as raiz_cnpj,
                                razao_social
                        from deltalaketrusted.receita_federal.empresas
                        where cnpj_raiz in ({cnpjs_str_4})
                    """

    empresas_1 = execute_query(conn, query_empresas_1)
    empresas_2 = execute_query(conn, query_empresas_2)
    empresas_3 = execute_query(conn, query_empresas_3)
    empresas_4 = execute_query(conn, query_empresas_4)

    empresas = pd.concat([empresas_1, empresas_2, empresas_3, empresas_4], ignore_index=True)

    empresas['raiz_cnpj'] = '00000000' + empresas['raiz_cnpj'].astype(str)
    empresas['raiz_cnpj'] = empresas['raiz_cnpj'].str[-8:]

    print(empresas)

    df_final = pd.merge(
        df_final, 
        empresas, 
        on=['raiz_cnpj'], 
        how='left'
    )

    print('Após JOIN com a Empresas:')
    print(df_final)

    df_final['razao_social'] = df_final['razao_social'].fillna('X')

    # Reorganizando Colunas

    colunas_ordem = ['raiz_cnpj', 'razao_social', 'unidade_consolidada', 'cidade', 'uf', 'cnae_principal'] + [col for col in df_final.columns if col not in ['raiz_cnpj', 'razao_social', 'unidade_consolidada', 'cidade', 'uf', 'cnae_principal']]
    df_final = df_final[colunas_ordem]

    print('Exportando base...')

    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_final['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_final['year'], df_final['month'], df_final['day'] = now.year, now.month, now.day

    df_final = df_final.reset_index(drop=True)


    # Exportando dados para a camada Trusted
    # # Conectando na Trusted
    storage_options = {
        "AWS_ACCESS_KEY_ID": Variable.get("MINIO_TRUSTED_ACCESS_KEY"),
        "AWS_SECRET_ACCESS_KEY": Variable.get("MINIO_TRUSTED_SECRET_KEY"),
        "AWS_ENDPOINT_URL": f"https://{Variable.get('MINIO_TRUSTED_ENDPOINT')}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_TRUSTED = 'payments'
    FOLDER_DESTINATION_TRUSTED = 'faturamento_externo/arcelor/base_atual'

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}", 
        df_final, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )
