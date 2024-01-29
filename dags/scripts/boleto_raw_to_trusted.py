import pandas as pd
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
from deltalake import write_deltalake
from scripts.query_trino import query_trino

now = datetime.now(tz=timezone(timedelta(hours=-3)))
print(now)

def transform_data_to_trusted(files_list, access_params):

    # DEFINE VARIABLES
    BUCKET_SOURCE_RAW = access_params['opdb_bucket']
    BUCKET_SOURCE_TRUSTED = "payments"
    BOLETOS_TRUSTED_FOLDER  = "boletos/"

    df_boletos_raw = pd.DataFrame()
    df_sacados_raw = pd.DataFrame()

    print(f"minio_params INSIDE: {access_params}")

    # CONECTAR NO MINIO RAW
    client = Minio(
        access_params['endpoint_url_raw'],
        access_key = access_params['aws_access_key_id_raw'],
        secret_key = access_params['aws_secret_access_key_raw'],
    )

    # READ FILES AND TRANSFORM THEM TO DATAFRAME
    for file_name in files_list:

        print(f"file_name: {file_name}")

        file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, 
                                 object_name=file_name)
        
        df_boletos_raw = pd.read_parquet(BytesIO(file.data))

        df_boletos_raw = pd.concat([df_boletos_raw, df_boletos_raw], ignore_index=True)

    df_boletos_padrao = pd.DataFrame()

    for k, v in df_boletos_raw['after'].items():
        df_boletos_temp = pd.DataFrame.from_dict(v, orient='index').T
        df_boletos_padrao = pd.concat([df_boletos_padrao, df_boletos_temp], ignore_index=True)
        
    df_boletos_padrao.set_index('id', inplace=True)

    ids_query = str(df_boletos_padrao['sacado_id'].tolist()).replace('[', '(').replace(']', ')')

    query = f"""
            select id, nome_sacado, numero_cnpj_sacado from postgres.ccred_schema_{access_params['stage']}_default.sacado where id IN {ids_query}
    """
    print(f"query: {query}")

    df_sacados_raw = query_trino(query, 
                                access_params['trino_endpoint'],
                                access_params['trino_port'],
                                access_params['trino_user'],
                                access_params['trino_password'])

    ''''
        A partir daqui, o código é o mesmo do script de transformação do raw para o trusted.
    '''
    # filtrar os boletos que realmente estão disponíveis para serem contabilizados
    '''
        codigo_cedente == 11
        excluido == 0 # deleçao lógica
        codigo_estagio_titulo == 6 `# dado já autorizado no qprof
    '''
    df_boletos_filtrado = df_boletos_padrao[(df_boletos_padrao['codigo_cedente'] == 11) 
                        & (df_boletos_padrao['excluido'] == 0) 
                        & (df_boletos_padrao['codigo_estagio_titulo'] == 6)]

    # Filtrar campos relevantes
    df_boletos_filtrado = df_boletos_filtrado[['sacado_id', 'numero_nfe', 'numero_titulo',  
                                       'data_emissao', 'data_vencimento', 'valor_baixado', 
                                       'data_baixa', 'valor_face', 'last_modified_date', 'codigo_cedente_endossante']]

    
    # Filtrar tabela sacados para usar os dados de cnpj e razao social
    df_sacados_filtrado = df_sacados_raw[['id', 'numero_cnpj_sacado', 'nome_sacado']]

    # mesclar os dados para ter o cnpj e a razao social do sacado
    df_boletos_final = df_boletos_filtrado.merge(df_sacados_filtrado, how='left', left_on='sacado_id', right_on='id')

    # criar a variável numero_parcela
    df_boletos_final['numero_parcela'] = df_boletos_final.sort_values(['numero_nfe', 'data_vencimento']) \
                .groupby(['numero_nfe']) \
                .cumcount() + 1
    df_boletos_final['numero_parcela'].fillna(1.0, inplace=True)
    df_boletos_final['numero_parcela'] = df_boletos_final['numero_parcela'].astype(int)

    # criar variável fornecedor
    df_boletos_final['fornecedor'] = 'alpe'

    # criar variável source
    df_boletos_final['fonte'] = 'Boletos Alpe'

    # criar variavel atualziado em
    df_boletos_final['atualizado_em'] = now

    # criar variavel tipo de documento - TO DO: todos documentos são CNPJ?
    df_boletos_final['tipo_documento'] = 'CNPJ'

    # ordenar as colunas
    df_boletos_final = df_boletos_final[['numero_cnpj_sacado', 'nome_sacado', 'numero_titulo', 
                  'data_emissao', 'data_vencimento', 'data_baixa', 
                  'valor_face', 'last_modified_date', 'numero_parcela', 'valor_baixado',
                    'fornecedor', 'fonte', 'atualizado_em', 'tipo_documento']]
    
    # renomear as colunas
    df_boletos_final.columns = ['documento', 'razao_social', 'numero_titulo', 
                            'data_emissao', 'data_vencimento', 'data_pagamento', 
                            'valor_titulo', 'data_hp', 'numero_parcela', 'valor_pago',
                            'fornecedor', 'fonte', 'atualizado_em', 'tipo_documento']


    # transformar o documento em string
    df_boletos_final['documento'] = df_boletos_final['documento'].astype(str)
     
    # tratar colunas de data
    date_cols = ['data_emissao', 'data_vencimento', 'data_pagamento', 'data_hp']

    for col in date_cols:
        # convert to to timestamp[us]
        df_boletos_final[col] = df_boletos_final[col].astype('datetime64[us]')
        # df_boletos_final[col] = pd.to_datetime(df_boletos_final[col], errors='coerce')

    # tratar colunas de valor
    value_cols = ['valor_titulo', 'valor_pago']

    for col in value_cols:
        df_boletos_final[col] = df_boletos_final[col].astype(float)

    '''
        ADICIONAR COLUNAS DE METADADOS
    '''
    df_boletos_final['year'] = now.year
    df_boletos_final['month'] = now.month
    df_boletos_final['day'] = now.day

    '''
        CONVERTER TIPOS DE COLUNAS
    '''
    convert_dict = {'documento': str,
            'razao_social': str,
            'numero_titulo': str,
            'data_emissao': 'datetime64[us]',
            'data_vencimento': 'datetime64[us]',
            'data_pagamento': 'datetime64[us]',
            'valor_titulo': float,
            'data_hp': 'datetime64[us]',
            'numero_parcela': int,
            'valor_pago': str,
            'fornecedor': str,
            'fonte': str,
            'atualizado_em': 'datetime64[us, UTC-03:00]',
            'tipo_documento': str,
            'year': int,
            'month': int,
            'day': int}
    
    for k, v in convert_dict.items():
        df_boletos_final[k] = df_boletos_final[k].astype(v)

    df_boletos_final.replace({'nan': None}, inplace=True)

    '''
        ENVIAR OS DADOS PARA O MINIO TRUSTED NO FORMATO DE DELTA TABLE
    '''
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL":f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    write_deltalake(f"s3a://{BUCKET_SOURCE_TRUSTED }/{BOLETOS_TRUSTED_FOLDER}", 
                    df_boletos_final, 
                    partition_by=["year", "month", "day"],
                    storage_options=storage_options,
                    mode="append",
                    )