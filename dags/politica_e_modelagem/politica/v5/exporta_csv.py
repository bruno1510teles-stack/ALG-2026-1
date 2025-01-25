# Carregando libs
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from datetime import datetime, timezone, timedelta
import re
import logging
from airflow.utils.log.logging_mixin import LoggingMixin
from io import BytesIO
import time
import boto3


def exporta_csv_cria_proposta_jira (access_params=None,  **kwargs):

    # Configuração da conexão S3/MinIO
    storage_options = {
        "aws_access_key_id": 'nr0qPLaAcdCtt7lAV4oa',
        "aws_secret_access_key": 'GRA8FxnVMy7pGDvKP1wZK2nPOC3vP7F1AvH2u3Ch',
        "endpoint_url": "https://api-trusted.alpe.com.br",
        "region_name": "us-east-1",
    }

    # Conectando ao S3/MinIO
    s3 = boto3.client("s3", **storage_options)

    # Definindo bucket e caminho do arquivo
    BUCKET_SOURCE_TRUSTED = "pre-aprovado-lote"
    FOLDER_DESTINATION_TRUSTED = "aprovados"

    # Gerando a data atual no formato desejado (ex: '13012025')
    current_date = datetime.now().strftime("%d%m%Y")
    # Definindo o nome do arquivo com a data automatizada
    file_name = f"resposta_politica_v5_{current_date}.xlsx"

    object_key = f"{FOLDER_DESTINATION_TRUSTED}/{file_name}"

    # Fazendo o download do arquivo Excel
    file_buffer = BytesIO()
    s3.download_fileobj(Bucket=BUCKET_SOURCE_TRUSTED, Key=object_key, Fileobj=file_buffer)
    file_buffer.seek(0)  # Reposiciona o ponteiro para o início do arquivo

    # Carregando o Excel em um DataFrame
    base_analisar = pd.read_excel(file_buffer)


    base_analisar['documento_sem_formatacao'] = base_analisar['documento_sem_formatacao'].astype(str).str.zfill(14)
    base_analisar['cnpj_raiz'] = base_analisar['cnpj_raiz'].str.slice(0, 8).str.zfill(8)
    
    print(f"Quantidade de CNPJs na base_analisar: {base_analisar.shape[0]}")

    df = base_analisar.head().copy()

    # Adicionando colunas extras com os valores fixos para exportar
    df['volume'] = 0
    df['limite'] = 30000
    df[['codigo_filial', 'uf', 'cdb_dba', 'vendedor_alpe', 'vendedor_fn_nome', 'vendedor_fn_email', 'vendedor_fn_telefone']] = ""
    df['prioridade'] = 6
    df['policy'] = 'V5'
    df['pre_filtro'] = 'Não'
    df['bucket_pgid'] = 'urn-party-pgid-arcelor' #Apenas para casos da arcelor
    df['cnpj_cedente'] = '17469701000177' #Apenas para casos da arcelor


    # Organiza base exportação
    exporta_csv = df[[
        'documento_sem_formatacao', 'volume', 'limite', 'razao_social', 'cnpj_cedente', 
        'codigo_filial', 'uf', 'cdb_dba', 'vendedor_alpe', 'vendedor_fn_nome', 
        'vendedor_fn_email', 'vendedor_fn_telefone', 'prioridade', 'policy', 'pre_filtro', 'bucket_pgid'
    ]]

    exporta_csv = exporta_csv.drop_duplicates()


    # Agrupando por Bucket PGID
    exporta_csv_pgid = exporta_csv.groupby('bucket_pgid')


        # Configuração do cliente MinIO
    minio_client = Minio(
        endpoint = "minio-api.alpe.tech",
        access_key = "dhHGmnBBi0ZPUlNWQFPn",
        secret_key = "wI9KSfQpqwpPWyGlouSzxD7Zp9YhLRPkfDZugsG5"
    )

    # Connection validation
    try:
        # Try to list the buckets
        buckets = minio_client.list_buckets()
        
        # If the connection was successful, print the buckests
        print("Conexão bem-sucedida. Lista de buckets disponíveis:")
        for bucket in buckets:
            print(bucket.name)
        
    except Exception as e:
            # If the connection was failed, print the error message
            print(f"Erro ao conectar ao MinIO: {e}")


    # Lista para armazenar buckets inexistentes
    buckets_inexistentes = []

    # Exportando cada grupo como CSV para o MinIO
    try:
        for bucket_pgid, grupo in exporta_csv_pgid:
            # Verificar se o bucket existe
            if not minio_client.bucket_exists(bucket_pgid):
                buckets_inexistentes.append(bucket_pgid)
                continue

            # Remover a coluna 'bucket_pgid' antes de exportar
            grupo = grupo.drop('bucket_pgid', axis=1)

            # Gerar o arquivo CSV em memória
            csv_buffer = BytesIO()
            grupo.to_csv(csv_buffer, sep=';', index=False, encoding='utf-8', header=False)
            csv_buffer.seek(0)  # Voltar ao início do arquivo

            # Nome do arquivo e caminho
            object_name = f"politica-credito/direcionamento-analise/in/{bucket_pgid}.csv"  # Criar pasta "in" e nome do arquivo

            # Carregar o arquivo para o MinIO no bucket correto
            minio_client.put_object(
                bucket_name=bucket_pgid,
                object_name=object_name,
                data=csv_buffer,
                length=csv_buffer.getbuffer().nbytes,
                content_type='text/csv'
            )

            print(f"Arquivo {object_name} exportado para o bucket {bucket_pgid} com {len(grupo)} linhas.")
        
        # Verificação após exportação
        if buckets_inexistentes:
            print("Os seguintes buckets não existem no MinIO. Processo será interrompido:")
            for bucket in buckets_inexistentes:
                print(f"- {bucket}")
            # Interrompe o processo
            raise Exception("Processo interrompido!")

    except Exception as e:
        print(f"{e}")


    # Timer de 1 minuto no final
    print("Aguardando alguns minutos antes de rodar o proximo processo...")
    time.sleep(800)  # Aguardar xx segundos
