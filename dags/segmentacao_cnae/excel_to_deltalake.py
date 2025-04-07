# Importing Libs
from minio import Minio
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta, timezone
from deltalake import write_deltalake

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

    
     # Gerando nome do arquivo para importacao
    BUCKET_SOURCE_RAW = "arquivos-python"
    FOLDER_DESTINATION_RAW = 'depara_cnae'
    file_name = 'Enriquecimento Segmento.xlsx'
    file_path = f'{FOLDER_DESTINATION_RAW}/{file_name}'


    # Carregando Excel
    response = minio_raw.get_object(BUCKET_SOURCE_RAW, file_path)
    file_data = BytesIO(response.read())
    df = pd.read_excel(file_data)


    df['Raíz CNPJ'] = df['Raíz CNPJ'].astype(str)
    df['CNPJ Completo'] = df['CNPJ Completo'].astype(str)
    df['CNAE'] = df['CNAE'].astype(str).str.replace('.0', '', regex=False)

    # 8 Digitos Raiz CNPJ
    df['Raíz CNPJ'] = df['Raíz CNPJ'].astype(str).str.zfill(8)

    df = df.rename(columns={'Raíz CNPJ': 'raiz_cnpj'})

    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df['year'], df['month'], df['day'] = now.year, now.month, now.day

    # Exportando dados para a camada Raw
    
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_raw'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_raw'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_raw']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_RAW = "arquivos-python/depara_cnae/delta"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_RAW}", 
        df, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )