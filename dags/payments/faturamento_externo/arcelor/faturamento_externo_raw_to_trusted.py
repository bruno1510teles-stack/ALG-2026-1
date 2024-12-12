# Importing Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake


def extracao_faturamento_externo(access_params=None, **kwargs):

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
    BUCKET_SOURCE_RAW = "faturamento-externo"
    FOLDER_DESTINATION_RAW = 'arcelor/year=2024/month=12/day=12'
    file_name = 'Faturamento Base dez24 - Tratada.xlsx'
    file_path = f'{FOLDER_DESTINATION_RAW}/{file_name}'


    # Uploading Excel File
    response = minio_raw.get_object(BUCKET_SOURCE_RAW, file_path)
    file_data = BytesIO(response.read())
    df = pd.read_excel(file_data, sheet_name="Histórico de Faturamento", header=1)


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

    format_column_names(df)

    df = df.dropna(subset=['Razão Social'])

    print('Parte 1')

    # Adjusting columns with upper()
    df['Razão Social'] = df['Razão Social'].str.upper()
    df['Unidade'] = df['Unidade'].str.upper()


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

    tratar_colunas_202(df)


    print('Parte 2')

    # Treating NaN values
    def fillna_in_columns_starting_with(df, prefix, value):
        # Filtering columns
        filtered_columns = [col for col in df.columns if str(col).startswith(prefix)]
        
        df[filtered_columns] = df[filtered_columns].fillna(value)

    fillna_in_columns_starting_with(df, '20', 0)


    #Transforming float64 in float
    df[df.select_dtypes(include=['float64']).columns] = df.select_dtypes(include=['float64']).astype(float)


    #Transforming ['Pagador']
    df['Pagador'] = df['Pagador'].astype(str)


    #Transforming ['Raiz CNPJ']
    df['Raiz CNPJ'] = '00000000' + df['Raiz CNPJ'].astype(str)
    df['Raiz CNPJ'] = df['Raiz CNPJ'].str[-8:]


    print('Parte 3')

    # Removendo coluna antiga de Raiz CNPJ antes do tratamento
    df = df.drop(columns=["Raiz CNPJ Recebido", "Pagador"])

    # Renomeando colunas
    df.rename(columns={'Raiz CNPJ': 'raiz_cnpj', 'Razão Social': 'razao_social', 'Unidade':'unidade'}, inplace=True)

    # Atribuindo data
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df['year'], df['month'], df['day'] = now.year, now.month, now.day

    print('Parte 4')

    df = df.reset_index(drop=True)


    # Exportando dados para a camada Trusted
    # # Conectando na Trusted
    storage_options = {
        "AWS_ACCESS_KEY_ID": 'nr0qPLaAcdCtt7lAV4oa',
        "AWS_SECRET_ACCESS_KEY": 'GRA8FxnVMy7pGDvKP1wZK2nPOC3vP7F1AvH2u3Ch',
        "AWS_ENDPOINT_URL":"https://api-trusted.alpe.com.br",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_TRUSTED = 'payments'
    FOLDER_DESTINATION_TRUSTED = 'faturamento_externo/arcelor'

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}/{FOLDER_DESTINATION_TRUSTED}", 
        df_dados, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )