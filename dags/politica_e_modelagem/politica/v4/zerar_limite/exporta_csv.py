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
import re
import time


def execucao_politica_zerar_limites_v4 (access_params=None,  **kwargs):

    # Conectando com o Trino
    conn = connect(
        host=access_params['trino_endpoint'],
        port=access_params['trino_port'],
        user=access_params['trino_user'],
        auth=BasicAuthentication(access_params['trino_user'], access_params['trino_password']),
        http_scheme="https"
    )

    # Função para execução da query
    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        return pd.DataFrame(rows, columns=columns)
    
    # Query Limites
    query_limites = f""" 
                    select cnpj_sacado as cnpj_sacado_completo,
                    substr(cnpj_sacado, 1, 8) as cnpj_sacado_raiz,
                    cedente as pgid,
                    limite_atribuido,
                    limite_utilizado,
                    limite_disponivel
                    from deltalaketrusted.limites.limite
                    where limite_atribuido > 0
                    """

    limites = execute_query(conn, query_limites)

    # Extraindo os CNPJs do DataFrame 'limites' e convertendo-os para uma lista
    cnpjs = limites['cnpj_sacado_raiz'].unique().tolist()
    # Convertendo a lista para uma string no formato adequado para o SQL
    cnpjs_str = ', '.join([f"'{cnpj}'" for cnpj in cnpjs])

    # Query Situação Receita
    query_receita =  f""" 
                    select distinct 
                        cnpj_raiz as cnpj_sacado_raiz,
                        situacao_cadastral
                    from deltalaketrusted.receita_federal.estabelecimentos
                    where is_matriz = true
                    and cnpj_raiz in ({cnpjs_str})
                    """

    receita = execute_query(conn, query_receita)

    # Query PEP e RJ
    query_pep_rj =  f""" 
                    select distinct
                        cnpj_raiz as cnpj_sacado_raiz,
                        razao_social as razao_social_sacado,
                        situacao_especial,
                        tem_pep
                    from deltalakerefined.motor.pre_filtro
                    where cnpj_raiz in ({cnpjs_str})
                    """

    pep_rj = execute_query(conn, query_pep_rj)


    # Unindo as informações (Chave: cnpj_sacado_raiz)
    # -> Limites e Situação Receita

    join1 = pd.merge(limites, receita, on='cnpj_sacado_raiz', how='left')

    # -> Limites e PEP/RJ
    df = pd.merge(join1, pep_rj, on='cnpj_sacado_raiz', how='left')

    # Filtrando casos para vencer
    df_vencer = df.loc[
        (df['situacao_cadastral'] != 'ATIVA') | 
        (df['situacao_especial'] == 'RECUPERACAO JUDICIAL') | 
        (df['tem_pep'] == True)
    ].reset_index(drop = True)


    # Cruzando com o cnpj do cedente

    empresa_cnpjs = {
        'agrichem': ['03860998000192'],
        'asusimplementos': ['10303297000118'],
        'ceufer': ['04117852000114'], # Confirmar
        'ciatintas': ['05419552000152'], # Confirmar
        'discor': ['09521591000117'],
        'megaleste': ['13571969000164'],
        'cadubo': ['28138113000177'],
        'arcelor': ['17469701000177']
    }

    cedentes = pd.DataFrame([
        {'pgid': empresa, 'cnpj_cedente': cnpj}
        for empresa, cnpjs in empresa_cnpjs.items()
        for cnpj in cnpjs
    ])


    df_vencer = pd.merge(df_vencer, cedentes, on='pgid', how='left')


    # Adicionando colunas extras com os valores fixos para exportar
    df_vencer['volume'] = 0
    df_vencer['limite'] = 0
    df_vencer[['codigo_filial', 'uf', 'cdb_dba', 'vendedor_alpe', 'vendedor_fn_nome', 'vendedor_fn_email', 'vendedor_fn_telefone']] = ""
    df_vencer['prioridade'] = 6
    df_vencer['policy'] = 'V4'
    df_vencer['pre_filtro'] = 'Não'
    df_vencer['bucket_pgid'] = 'urn-party-pgid-' + df_vencer['pgid']


    # Organiza base exportação
    exporta_csv = df_vencer[[
        'cnpj_sacado_completo', 'volume', 'limite', 'razao_social_sacado', 'cnpj_cedente', 
        'codigo_filial', 'uf', 'cdb_dba', 'vendedor_alpe', 'vendedor_fn_nome', 
        'vendedor_fn_email', 'vendedor_fn_telefone', 'prioridade', 'policy', 'pre_filtro', 'bucket_pgid'
    ]]

    exporta_csv = exporta_csv.drop_duplicates()

    # Para teste em produção
    exporta_csv = exporta_csv[:1]

    # Agrupando por Bucket PGID
    exporta_csv_pgid = exporta_csv.groupby('bucket_pgid')


        # Configuração do cliente MinIO
    minio_client = Minio(
            "minio-api.alpenet.com.br",
            access_key="pe4MdrBZnRqrLUatARfZ",
            secret_key="d7RdWy02br3Q9Tsvmq8rOXDI9buAWlurPATmTFZh",
            secure=True
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
    print("Aguardando 30 minutos antes de rodar o proximo processo...")
    time.sleep(300)  # Aguardar 60 segundos (1 minuto)
