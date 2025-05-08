# Carregando libs
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from minio import Minio
from deltalake import write_deltalake
from datetime import datetime, timezone, timedelta
from io import BytesIO
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import Variable

def limites_to_raw(access_params=None, **kwargs):

    ### Coletando dados da camada Raw
    # Conectando com o banco
    client = Minio(
            "minio-api.alpenet.com.br",
            access_key="pe4MdrBZnRqrLUatARfZ",
            secret_key="d7RdWy02br3Q9Tsvmq8rOXDI9buAWlurPATmTFZh",
            secure=True
    )

    # Função para encontrar o arquivo mais recente
    def encontrar_arquivo_mais_recente(client, bucket_name, prefix):
        try:
            # Lista todos os objetos com o prefixo dado
            objetos = client.list_objects(bucket_name, prefix=prefix, recursive=True)
            
            # Inicializa as variáveis para encontrar o arquivo mais recente
            arquivo_mais_recente = None
            ultima_modificacao = None
            
            # Itera sobre os objetos
            for obj in objetos:
                # Converte a data de modificação para datetime
                data_modificacao = obj.last_modified
                
                # Se for o primeiro objeto ou se a data for mais recente, atualiza o arquivo mais recente
                if ultima_modificacao is None or data_modificacao > ultima_modificacao:
                    arquivo_mais_recente = obj
                    ultima_modificacao = data_modificacao
            
            # Retorna o arquivo mais recente ou None se não houver arquivos
            return arquivo_mais_recente
        
        except Exception as e:
            print(f"Erro ao procurar arquivos em {prefix}: {e}")
            return None



    # Lista de prefixos (pastas principais)
    buckets_1 = [
        "urn-party-pgid-agrichem",
        "urn-party-pgid-asusimplementos",
        "urn-party-pgid-ceufer",
        "urn-party-pgid-ciacor",
        "urn-party-pgid-ciatintas",
        "urn-party-pgid-discor",
        "urn-party-pgid-megaleste",
        "urn-party-pgid-belgo",
        "urn-party-pgid-gonvarri",
        "urn-party-pgid-agrichem"
    ]

    # Lista de prefixos (pastas principais)
    buckets_2 = [
        "urn-party-pgid-cadubo"
    ]


    # Lista de prefixos (pastas principais)
    buckets_3 = [
        "urn-party-pgid-arcelor"
    ]


    # Caminho dentro de cada prefixo que queremos buscar
    subcaminho = "limite/out/"

    # Criando DF e nome colunas
    v1 = pd.DataFrame()
    v2 = pd.DataFrame()
    v3 = pd.DataFrame()
    colunas_v1 = ['cnpj_sacado', 'limite_atribuido', 'limite_utilizado', 'limite_disponivel', 'stop_supply']
    colunas_v2 = ['cnpj_sacado', 'limite_atribuido', 'limite_utilizado', 'limite_disponivel', 'status', 'stop_supply', 'motivo_bloqueio']
    colunas_v3 = ['cnpj_sacado', 'limite_atribuido', 'limite_utilizado', 'limite_disponivel', 'bloqueado','razao_social_sacado', 'data_ultima_operacao']

    # Função para processar os buckets e concatenar em um DataFrame
    def processar_buckets(client, buckets, colunas, df):
        for bucket in buckets:
            caminho_completo = subcaminho  # Caminho dentro do bucket
            
            # Encontra o arquivo mais recente no bucket atual
            arquivo = encontrar_arquivo_mais_recente(client, bucket, caminho_completo)
            
            if arquivo:
                print(f"Arquivo mais recente em {bucket}/{caminho_completo}: {arquivo.object_name}, Modificado em: {arquivo.last_modified}")
                
                # Baixa o arquivo mais recente
                data = client.get_object(bucket, arquivo.object_name)
                
                # Lê o arquivo como um DataFrame (ajuste o `pd.read_csv` conforme o formato do arquivo)
                df_bucket = pd.read_csv(BytesIO(data.read()), sep=';', header=None, names=colunas)
                
                # Fecha o objeto baixado
                data.close()
                
                # Extrai o nome final do bucket (ex: "agrichem" de "urn-party-pgid-agrichem")
                nome_bucket = bucket.split("-")[-1]
                
                # Adiciona uma nova coluna ao DataFrame com o nome do bucket
                df_bucket['cedente'] = nome_bucket
                
                # Concatena com o DataFrame final
                df = pd.concat([df, df_bucket], ignore_index=True)
            else:
                print(f"Nenhum arquivo encontrado em {bucket}/{caminho_completo}")
        return df

    # Processa cada conjunto de buckets e armazena em seus respectivos DataFrames
    v1 = processar_buckets(client, buckets_1, colunas_v1, v1)
    v2 = processar_buckets(client, buckets_2, colunas_v2, v2)
    v3 = processar_buckets(client, buckets_3, colunas_v3, v3)

    # Junta todos os DataFrames em um só
    df_final = pd.concat([v1, v2, v3], ignore_index=True)

    def substituir_virgula_ponto(coluna):
        # Converte para string, substitui vírgulas por pontos e tenta converter para float
        return pd.to_numeric(coluna.astype(str).str.replace(',', '.'), errors='coerce')

    # Aplicar a função nas colunas específicas
    df_final['limite_atribuido'] = substituir_virgula_ponto(df_final['limite_atribuido'])
    df_final['limite_utilizado'] = substituir_virgula_ponto(df_final['limite_utilizado'])
    df_final['limite_disponivel'] = substituir_virgula_ponto(df_final['limite_disponivel'])

    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_final['atualizado_em'] = now.strftime('%Y-%m-%d %X')
    df_final['year'], df_final['month'], df_final['day'] = now.year, now.month, now.day

    # Conta o número de linhas por bucket
    contagem_por_bucket = df_final.groupby('cedente').size().reset_index(name='contagem')

    # Exibe o resultado
    print(contagem_por_bucket)

    # Exportando dados para a camada Raw
        
    storage_options = {
        "AWS_ACCESS_KEY_ID": access_params['aws_access_key_id_raw'],
        "AWS_SECRET_ACCESS_KEY": access_params['aws_secret_access_key_raw'],
        "AWS_ENDPOINT_URL": f"https://{access_params['endpoint_url_raw']}",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    # Definindo o caminho e salvando no MinIO
    BUCKET_SOURCE_TRUSTED = "limites"

    write_deltalake(
        f"s3a://{BUCKET_SOURCE_TRUSTED}", 
        df_final, 
        partition_by=["year", "month", "day"],
        storage_options=storage_options,
        mode="overwrite"
    )
