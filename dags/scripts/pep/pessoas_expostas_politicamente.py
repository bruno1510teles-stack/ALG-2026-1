# Carregando libs
import pandas as pd
import pyarrow as pa
from datetime import datetime, timezone, timedelta
from minio import Minio
from io import BytesIO
from deltalake import write_deltalake, DeltaTable
import psutil


# Criando conexão
def transform_pep_to_trusted(files_list_pep, access_params):

    # Variaveis Conexão
    BUCKET_SOURCE_RAW = "pessoas-expostas-politicamente"
    BUCKET_SOURCE_TRUSTED = "pessoas-e-organizacoes"
    TRUSTED_FOLDER = "pep"

    # Conectando na raw
    client = Minio(
        access_params['endpoint_url_raw'],
        access_key = access_params['aws_access_key_id_raw'],
        secret_key = access_params['aws_secret_access_key_raw'],
    )


    nome_colunas = ['documento', 'nome', 'sigla_funcao', 'funcao', 'nivel_funcao', 'nome_orgao', 'data_inicio_exercicio', 'data_fim_exercicio', 'data_fim_carencia']

    print('Importando arquivo')
    # Juntando arquivos da Motor
    file_name = files_list_pep[0]
    print(f"file_name: {file_name}")
    file = client.get_object(bucket_name=BUCKET_SOURCE_RAW, object_name=file_name)
    df = pd.read_csv(BytesIO(file.data), encoding='latin1', sep=';',header=0, dtype=str, names=nome_colunas)

    print(file_name)
    print('Definindo ano ref')
    df['ano_ref'] = file_name[-14:-10]
    df['mes_ref'] = file_name[-10:-8]
    
    print('Definindo particoes trusted')
    now = datetime.now(tz=timezone(timedelta(hours=-3)))

    df['atualizado_em'] = now
    df['atualizado_em'] = now.strftime('%Y-%m-%d %X')  

    df['year'] = now.year
    df['month'] = now.month
    df['day'] = now.day
    
    print('Transformando string em data')
    df.loc[df['data_inicio_exercicio'] == 'Não informada', 'data_inicio_exercicio'] = None
    df.loc[df['data_fim_exercicio'] == 'Não informada', 'data_fim_exercicio'] = None
    df.loc[df['data_fim_carencia'] == 'Não informada', 'data_fim_carencia'] = None

    df['data_inicio_exercicio'] = pd.to_datetime(df['data_inicio_exercicio'], dayfirst=True).dt.date
    df['data_fim_exercicio'] = pd.to_datetime(df['data_fim_exercicio'], dayfirst=True).dt.date
    df['data_fim_carencia'] = pd.to_datetime(df['data_fim_carencia'], dayfirst=True).dt.date
    
    
    schema = pa.schema([
    ('documento', pa.string()),
    ('nome', pa.string()),
    ('sigla_funcao', pa.string()),
    ('funcao', pa.string()),
    ('nivel_funcao', pa.string()),
    ('nome_orgao', pa.string()),
    ('data_inicio_exercicio', pa.date64()),
    ('data_fim_exercicio', pa.date64()),
    ('data_fim_carencia', pa.date64()),
    ('ano_ref', pa.string()),
    ('mes_ref', pa.string()),
    ('atualizado_em', pa.string()),
    ('year', pa.int32()),
    ('month', pa.int32()),
    ('day', pa.int32())
    ])
    
    df = pa.Table.from_pandas(df, preserve_index=False, schema=schema)    
    
    print(f"Salvando no Minio {psutil.virtual_memory()._asdict()}")
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_trusted'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_trusted'],
        "AWS_ENDPOINT_URL":f"https://{access_params['endpoint_url_trusted']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    
    write_deltalake(f"s3a://{BUCKET_SOURCE_TRUSTED}/{TRUSTED_FOLDER}", 
                    df, 
                    partition_by=["year", "month", "day"],
                    storage_options=storage_options,
                    mode="append",
                    )