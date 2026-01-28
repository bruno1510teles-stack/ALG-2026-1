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
from airflow.models import Variable
from tabulate import tabulate
import requests


def exporta_csv_politica_v4 (access_params=None,  **kwargs):

    conn = connect(
		host=Variable.get("TRINO_ENDPOINT"),
		port=Variable.get("TRINO_PORT"),
		user=Variable.get("TRINO_USER"),
		auth=BasicAuthentication(Variable.get("TRINO_USER"), Variable.get("TRINO_PASSWORD")),
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
                    where cnpj_raiz in ('')
                    """
    # Alterado, pois não estamos vencendo mais limites com a regra do PEP no momento, quando voltarmos substituir o '' por cnpjs_str

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
    df_vencer['variavel_coringa'] = 'PULAR FILTROS'
    df_vencer['bucket_pgid'] = 'urn-party-pgid-' + df_vencer['pgid']


    # Organiza base exportação
    exporta_csv = df_vencer[[
        'cnpj_sacado_completo', 'volume', 'limite', 'razao_social_sacado', 'cnpj_cedente', 
        'codigo_filial', 'uf', 'cdb_dba', 'vendedor_alpe', 'vendedor_fn_nome', 
        'vendedor_fn_email', 'vendedor_fn_telefone', 'prioridade', 'policy', 'pre_filtro', 'variavel_coringa', 'bucket_pgid'
    ]]

    exporta_csv = exporta_csv.drop_duplicates()

    df_vencer["MOTIVO DO CANCELAMENTO"] = (df_vencer["situacao_cadastral"].combine_first(df_vencer["situacao_especial"]))

    cancelamento_v4 = (
        df_vencer.groupby("MOTIVO DO CANCELAMENTO")
      .agg(
          QUANTIDADE=("cnpj_sacado_completo", "count"),
          LIMITE_ZERADO=("limite_atribuido", "sum")
      )
      .reset_index()
    )

    cancelamento_v4["LIMITE_ZERADO"] = cancelamento_v4["LIMITE_ZERADO"].apply(
        lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        if pd.notnull(x) and isinstance(x, (int, float)) else "R$ 0,00")
    
    if cancelamento_v4.empty:
        markdown = f"⚠️ Propostas V4 - Nenhum limite zerado"
    else:
    # Converte DF em tabela formatada
        tabela_formatada = tabulate(
            cancelamento_v4.values.tolist(),
            headers=cancelamento_v4.columns.tolist(),
            tablefmt="pretty"
        )

    # Espaço invisível (para espaçamento no Teams/Markdown)
    invisible_space = "\u200B"

    # Monta o markdown final
    markdown = (
        "📊 Propostas V4 - Relatório semanal de Cancelamento de Limite\n\n"
        "```\n" + tabela_formatada + "\n```\n"
        f"{invisible_space}\n"
    )

    print(markdown)

    def enviar_para_webhook(mensagem):
        webhook_url = "https://yandehbr.webhook.office.com/webhookb2/aff1add1-1e5e-445d-9644-f7d9ab677641@fe284b6f-c6d2-4028-badb-7d0c22aef0ae/IncomingWebhook/cd64a656b86b4db6a8a64a74153a8555/e3ad1a1a-7716-40ee-ab81-0f05650df5dc/V2M-crEG-kOlO8wQffCBWAHSBeR29YtNktVPx1gvoiR4M1"

        headers = {
   "Content-Type": "application/json"
  }

        payload = {
   "text": mensagem
  }

        response = requests.post(webhook_url, json=payload, headers=headers)

        if response.status_code == 200:
            print("Mensagem enviada com sucesso para o Teams!")
        else:
            print(f"Falha ao enviar a mensagem. Código de status: {response.status_code}")

    enviar_para_webhook(markdown)

    print(f"Quantidade de CNPJs para criar issue no Jira politica v4: {exporta_csv.shape[0]}")

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