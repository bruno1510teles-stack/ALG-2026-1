import pandas as pd
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
from deltalake import write_deltalake

now = datetime.now(tz=timezone(timedelta(hours=-3)))

def transform_data_to_trusted(files_list, access_params):

   # DEFINE VARIABLES
    BUCKET_SOURCE_RAW = "hp-externa"
    BUCKET_SOURCE_TRUSTED = "payments"
    HPEX_TRUSTED_FOLDER  = "boletos/"

    df_all = pd.DataFrame()

    # CONECTAR NO MINIO RAW
    client = Minio(
        access_params['endpoint_url_raw'],
        access_key = access_params['aws_access_key_id_raw'],
        secret_key = access_params['aws_secret_access_key_raw'],
    )

    # READ FILES AND TRANSFORM THEM TO DATAFRAME
    for file_name in files_list:

        file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, 
                                 object_name=file_name)
        
        df_hpex_raw_temp = pd.read_parquet(BytesIO(file.data))

        df_all = pd.concat([df_all, df_hpex_raw_temp], ignore_index=True)
    '''
        A partir daqui, o código é o mesmo do script de transformação do raw para o trusted.
    '''

    # TRANSFORM DATA
    # Transform documento column
    print("Before >>>>>>>>>>>>>>>>>")
    print( df_all['documento'].count())
    print( df_all['documento'].value_counts(dropna=False))

    # documento as string
    df_all['documento'] = df_all['documento'].astype(str)

    # Remove all non-numeric characters
    df_all['documento'] = df_all['documento'].str.replace(r'[^\d]', '', regex=True)

    # Função para verificar se é CPF ou CNPJ
    def verifica_cpf_cnpj(doc):
        if len(doc) > 11 or doc[-4:-7:-1] == "000"[::-1]:
            return "CNPJ"
        else:
            return "CPF"

    # Aplica a função à coluna 'documento' e cria uma nova coluna 'tipo_documento'
    df_all['tipo_documento'] = df_all['documento'].apply(verifica_cpf_cnpj)

    df_all['documento'] = df_all.apply(lambda x: x['documento'].zfill(14) if x['tipo_documento'] == 'CNPJ' else x['documento'].zfill(11), axis=1)

    # trocar para null se a coluna documento tiver 00000000000 ou 00000000000000
    df_all['documento'] = df_all['documento'].replace('00000000000', '')
    df_all['documento'] = df_all['documento'].replace('00000000000000', '')

    #df_all['contador'] = df_all['documento'].str.len()
    #df_all[['tipo_documento', 'contador']].value_counts(dropna=False)

    print("After >>>>>>>>>>>>>>>>>")
    print( df_all['documento'].count())
    print( df_all['documento'].value_counts(dropna=False))

    # TRATAMENTO DE CAMPOS MONETÁRIOS
    try:
        print("Somar de itens nulos dem valor_titulo ANTES da transformação: " + str(df_all['valor_titulo'].isna().sum()))
        df_all['valor_titulo'] = df_all['valor_titulo'].astype(str)

        # verifica se o número tem '.' como milhar e ',' como decimal e retira o '.'
        mask = df_all['valor_titulo'].str.contains('\..*,', na=False)
        df_all.loc[mask, 'valor_titulo'] = df_all.loc[mask, 'valor_titulo'].str.replace('\.', '', regex=True)

        # Troca virgula por ponto
        df_all['valor_titulo'] = df_all['valor_titulo'] .replace(',', '.', regex=True)
        
        df_all['valor_titulo'] = df_all['valor_titulo'].astype(float)
        print("Somar de itens nulos dem valor_titulo DEPOIS da transformação: " + str(df_all['valor_titulo'].isna().sum()))       
    except:
        print("valor_titulo não está presente")

    try:
        # Faz o mesmo da celula anterior mas como  valor_pago
        print("Somar de itens nulos dem valor_pago ANTES da transformação: "+ str(df_all['valor_pago'].isna().sum()))
        mask = df_all['valor_pago'].str.contains('\..*,', na=False)
        df_all.loc[mask, 'valor_pago'] = df_all.loc[mask, 'valor_pago'].str.replace('\.', '', regex=True)

        # Troca virgula por ponto
        df_all['valor_pago'] = df_all['valor_pago'] .replace(',', '.', regex=True)
            
        df_all['valor_pago'] = df_all['valor_pago'].astype(float)
        print("Somar de itens nulos dem valor_pago DEPOIS da transformação: "+ str(df_all['valor_pago'].isna().sum()))
    except:
        print("valor_pago não está presente")


    try:
        print("Somar de itens nulos dem saldo_aberto ANTES da transformação: "+ str(df_all['saldo_aberto'].isna().sum()))
        # Faz o mesmo da celula anterior mas como  saldo_aberto
        mask = df_all['saldo_aberto'].str.contains('\..*,', na=False)
        df_all.loc[mask, 'saldo_aberto'] = df_all.loc[mask, 'saldo_aberto'].str.replace('\.', '', regex=True)

        # Troca virgula por ponto
        df_all['saldo_aberto'] = df_all['saldo_aberto'] .replace(',', '.', regex=True)

        df_all['saldo_aberto'] = df_all['saldo_aberto'].astype(float)
        print("Somar de itens nulos dem saldo_aberto DEPOIS da transformação: "+ str(df_all['saldo_aberto'].isna().sum()))
    except:
        print("saldo_aberto não está presente")

    # trocar por vazio se data_pagamento for '1900-01-01 00:00:00'
    df_all['data_pagamento'] = df_all['data_pagamento'].replace('1900-01-01 00:00:00', '')

    df_all['data_vencimento'] = df_all['data_vencimento'].replace('2032-02-14 00:00:00', '2023-02-14 00:00:00') 

    try:
        # TRATAMENTO NUMERO PARCELAS
        df_all['numero_parcela'] = df_all['numero_parcela'].replace('-', np.NaN)
        df_all['numero_parcela'] = df_all['numero_parcela'].replace('', np.NaN)

        df_all['numero_parcela'] = df_all['numero_parcela'].fillna(0).astype('int')
    except:
        print("numero_parcela não está presente")

    # Criar uma nova coluna com o nome da fonte
    df_all['fonte'] = 'hpex'
    # adicionar momento do processamento
    df_all['atualizado_em'] = now

    print(df_all.info())

    df_all['year'] = now.year
    df_all['month'] = now.month
    df_all['day'] = now.day

    print(df_all.info())

    ''''
        FIM DO TRATAMENTO
    '''
    
    '''
        CONVERTER TIPOS DE COLUNAS
    '''
    convert_dict = {'documento': str,
            'razao_social': str,
            'numero_titulo': str,
            'data_emissao': str,
            'data_vencimento': str,
            'data_pagamento': str,
            'valor_titulo': float,
            'data_hp': str,
            'numero_parcela': int,
            'valor_pago': str,
            'fornecedor': str,
            'fonte': str,
            'atualizado_em': str,
            'tipo_documento': str,
            'year': int,
            'month': int,
            'day': int}
    
    for k, v in convert_dict.items():
        if k not in df_all.columns:
            df_all[k] = None
        df_all[k] = df_all[k].astype(v)


    # re order columns
    df_all = df_all[list(convert_dict.keys())]

    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL":f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    write_deltalake(f"s3a://{BUCKET_SOURCE_TRUSTED }/{HPEX_TRUSTED_FOLDER}", 
                    df_all, 
                    partition_by=["year", "month", "day"],
                    storage_options=storage_options,
                    mode="append",
                    )