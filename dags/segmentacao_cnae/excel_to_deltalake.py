# Importing Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake
from trino.dbapi import connect
from trino.auth import BasicAuthentication

def transforma_excel_deltalake_cnae (access_params=None, **kwargs):

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

    
   # Conexão com o banco de dados
    conn = connect(
        host='trino.alpe.com.br',
        port=443,
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
    

    # Gerando nome do arquivo para importacao
    BUCKET_SOURCE_RAW = "arquivos-python"
    FOLDER_DESTINATION_RAW = 'depara_cnae'
    file_name = 'Enriquecimento Segmento.xlsx'
    file_path = f'{FOLDER_DESTINATION_RAW}/{file_name}'
    

    # Carregando Excel
    response = minio_raw.get_object(BUCKET_SOURCE_RAW, file_path)
    file_data = BytesIO(response.read())
    df_enriquecimento = pd.read_excel(file_data)
    
    
    # Gerando nome do arquivo para importacao
    BUCKET_SOURCE_RAW_2 = "arquivos-python"
    FOLDER_DESTINATION_RAW_2 = 'depara_cnae'
    file_name_2 = 'CNAE_Subclasses_2_3_Estrutura_Detalhada.xlsx'
    file_path_2 = f'{FOLDER_DESTINATION_RAW_2}/{file_name_2}'
    

    # Carregando Excel
    response_2 = minio_raw.get_object(BUCKET_SOURCE_RAW_2, file_path_2)
    file_data_2 = BytesIO(response_2.read())
    df_cnae_fixo = pd.read_excel(file_data_2)
    
    
    query_sacados = f"""
            select
                substring(s.numero_cnpj_sacado, 1,8) as raiz_cnpj,
                s.numero_cnpj_sacado,
                s.numero_cnpj_sacado_formatado,
                s.nome_sacado,
                e.cnae_principal
            from
                postgres.ccred_schema_prd_default.sacado s
            inner join
                deltalaketrusted.receita_federal.estabelecimentos e
                on s.numero_cnpj_sacado = e.documento_sem_formatacao
    
    """
    
    
    
    query_segmento = f""" select 
                                cnpj_sacado as "CNPJ SACADO",
                                sum(case when nome_cedente = 'ARCELORMITTAL BRASIL S/A' or nome_cedente = 'BELGO BEKAERT ARAMES LTDA' 
                                    or nome_cedente = 'ARCELORMITTAL GONVARRI BRASIL PRODUTOS SIDERURGICOS S/A'  then 1 else 0 end) as casos_matcon,
                                sum(case when nome_cedente = 'CASAL COMERCIO E SERVICOS LTDA' or nome_cedente = 'CASA DO ADUBO S.A' 
                                    or nome_cedente = 'ASUS INDÚSTRIA DE MÁQUINAS AGRÍCOLAS LTDA' or nome_cedente = 'AGRICHEM DO BRASIL S.A'  then 1 else 0 end) as casos_agro,
                                sum(case when nome_cedente not in ( 'CASAL COMERCIO E SERVICOS LTDA', 'CASA DO ADUBO S.A', 'ASUS INDÚSTRIA DE MÁQUINAS AGRÍCOLAS LTDA',
                                                                    'AGRICHEM DO BRASIL S.A', 'ARCELORMITTAL BRASIL S/A', 'BELGO BEKAERT ARAMES LTDA',
                                                                    'ARCELORMITTAL GONVARRI BRASIL PRODUTOS SIDERURGICOS S/A') then 1 else 0 end) as casos_outros
                            from deltalaketrusted.payments.boletos_internos
                            group by cnpj_sacado
                    """
    

    if conn is not None:
        segmento = execute_query(conn, query_segmento)
        sacados = execute_query(conn, query_sacados)
        print("Dados carregados com sucesso.")
    else:
        print("A consulta não foi executada porque a conexão com o Trino falhou.")
    

    #Tratamento sacados
    sacados.rename(columns={'numero_cnpj_sacado': 'cnpj_completo', 'numero_cnpj_sacado_formatado': 'cnpj','cnae_principal': 'cnae'}, inplace=True)
    sacados['cnpj_completo'] = sacados['cnpj_completo'].astype(str).str.zfill(14)
    sacados['cnae'] = sacados['cnae'].fillna('').astype(str).str.replace('.0', '', regex=False).str.zfill(7)
    

    #Tratamentos colunas do Excel
    df_enriquecimento.rename(columns ={'Raiz CNPJ': 'raiz_cnpj', 'CNPJ Completo': 'cnpj_completo', 'CNPJ SACADO': 'cnpj','NOME SACADO': 'nome_sacado', 'CNAE': 'cnae', 'SEÇÃO': 'secao', 'DIVISÃO': 'divisao', 'Seção Final': 'secao_final', 'Divisão Final': 'divisao_final'},inplace=True)
    df_enriquecimento['raiz_cnpj'] = df_enriquecimento['raiz_cnpj'].astype(str).str.zfill(8)
    df_enriquecimento['cnpj_completo'] = df_enriquecimento['cnpj_completo'].astype(str).str.zfill(14)
    df_enriquecimento['cnae'] = df_enriquecimento['cnae'].astype(str).str.replace('.0', '', regex=False)
    

    df_merge = sacados.merge(
        df_enriquecimento[['cnpj_completo', 'secao_final', 'divisao_final']],
        on='cnpj_completo',
        how='left')
    

    df_cnae_fixo = df_cnae_fixo.rename(columns={'CNAE ajs': 'cnae','Seção.1': 'secao_desc', 'Divisão.1': 'divisao_desc'})
    

    #Tratamentos df_cnae_fixo
    df_cnae_fixo['cnae'] = df_cnae_fixo['cnae'].fillna('').astype(str).str.replace('.0', '', regex=False).str.zfill(7)
    

    df_merge = df_merge.merge(
        df_cnae_fixo[['cnae', 'secao_desc', 'divisao_desc']],
        on='cnae',
        how='left')
    

    #Regra: Se já existir a segmentação no Enriquecimento, considera ela, se não, usa a do df_cnae_fixo
    
    df_merge['secao_final'] = df_merge['secao_final'].fillna(df_merge['secao_desc'])
    df_merge['divisao_final'] = df_merge['divisao_final'].fillna(df_merge['divisao_desc'])
    

    df_merge = df_merge.drop(columns=['secao_desc', 'divisao_desc'])
    

    segmento['cnpj_completo'] = segmento['CNPJ SACADO'].astype(str).str.replace(r'[./-]', '', regex=True)
    

    df = pd.merge(df_merge, segmento[['cnpj_completo', 'casos_matcon', 'casos_agro', 'casos_outros']], on ='cnpj_completo', how = 'left')
    
    
    # Criando segmento e sub_segmento
    df['segmento'] = df.apply(
        lambda row: 'MATCON' if row['casos_matcon'] > 0 else
                    ('AGRO' if row['casos_agro'] > 0 else
                    ('OUTROS' if row['casos_outros'] > 0 else 'OUTROS')),
        axis=1
    )
    
    
    df['sub_segmento'] = df.apply(
        lambda row: 'COMÉRCIO VAREJISTA' if row['segmento'] == 'MATCON' and row['divisao_final'] in ['COMÉRCIO VAREJISTA', 'COMÉRCIO E REPARAÇÃO DE VEÍCULOS AUTOMOTORES E MOTOCICLETAS'] else
                    ('COMÉRCIO ATACADISTA' if row['segmento'] == 'MATCON' and row['divisao_final'] == 'COMÉRCIO POR ATACADO, EXCETO VEÍCULOS AUTOMOTORES E MOTOCICLETAS' else
                    ('INCORPORAÇÃO' if row['segmento'] == 'MATCON' and row['secao_final'] == 'CONSTRUÇÃO' else row['segmento'])),
        axis=1
    )
    
    
    df['sub_segmento'] = df.apply(lambda row: 'MATCON - OUTROS' if row['segmento'] == 'MATCON' and row['sub_segmento'] == 'MATCON' else row['sub_segmento'], axis=1)
    
    
    df = df.drop(columns=['casos_matcon', 'casos_agro', 'casos_outros'])
    
    # Setando 'OUTROS' para secao_final e divisao_final quando cnae == 8888888
    df.loc[df['cnae'] == '8888888', ['secao_final', 'divisao_final']] = 'OUTROS'
    
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df['year'], df['month'], df['day'] = now.year, now.month, now.day

    df = df.drop_duplicates()
    
    df = df.reset_index(drop=True)
    
    # Identificar registros não correspondidos
    nao_correspondidos = df[df['secao_final'].isna()]
    print(nao_correspondidos[['cnpj_completo','nome_sacado', 'cnae','secao_final', 'divisao_final']])
    
    print("DF FINAL:")
    print(df)
    
    # Exportando dados para a camada Raw
    
    storage_options = {
            "AWS_ACCESS_KEY_ID": "d8jOuN46ckGNsr6zzpyw",
            "AWS_SECRET_ACCESS_KEY": "gnyBdrsoDrRcM9ln0QO83Nw8I4TlOFDOI4J9QDKc",
            "AWS_ENDPOINT_URL": "https://api-refined.alpe.com.br",
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_TRUSTED = "cnae/depara-cnae/delta"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}", 
        df, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )