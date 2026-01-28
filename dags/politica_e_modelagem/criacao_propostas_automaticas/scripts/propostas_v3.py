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
from airflow.models import Variable
import requests
from tabulate import tabulate


def exporta_csv_politica_v3(access_params=None, **kwargs):
    # Conectando com o Trino
    conn = connect(
        host=Variable.get("TRINO_ENDPOINT"),
        port=Variable.get("TRINO_PORT"),
        user=Variable.get("TRINO_USER"),
        auth=BasicAuthentication(
            Variable.get("TRINO_USER"),
            Variable.get("TRINO_PASSWORD")
        ),
        http_scheme="https"
    )

    # Função para execução da query
    def execute_query(conn, query):
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()
        return pd.DataFrame(rows, columns=columns)

    # Query base boletos
    query_boletos = f"""
        SELECT
            numero_sequencial_titulo,
            codigo_filial,
            codigo_empresa,
            numero_titulo,
            codigo_cedente,
            cedente_id,
            codigo_sacado,
            sacado_id,
            status_titulo,
            status_liquidez,
            codigo_situacao_titulo,
            data_emissao,
            data_efetivacao,
            data_vencimento,
            data_baixa,
            CAST(valor_face AS DOUBLE) AS valor_face,
            CAST(valor_titulo AS DOUBLE) AS valor_titulo,
            CAST(valor_baixado AS DOUBLE) AS valor_baixado,
            CAST(valor_desagio AS DOUBLE) AS valor_desagio,
            rotulo,
            codigo_estagio_titulo,
            numero_nota_fiscal,
            numero_nfe,
            nome_fantasia_sacado,
            nome_sacado,
            cnpj_sacado,
            uf_sacado,
            cidade_sacado,
            cnpj_cedente,
            nome_cedente,
            nome_fantasia_cedente,
            safra_concessao,
            safra_vencimento,
            safra_baixa,
            atualizado_em,
            year,
            month,
            day
        FROM deltalaketrusted.payments.boletos_internos
    """

    boletos = execute_query(conn, query_boletos)

    # Query base limites
    query_limites = f"""
        SELECT
            cnpj_sacado,
            cedente AS pgid,
            SUM(limite_atribuido) AS limite_atribuido,
            SUM(limite_disponivel) AS limite_disponivel
        FROM deltalaketrusted.limites.limite
        GROUP BY cnpj_sacado, cedente
    """

    limites = execute_query(conn, query_limites)

    # Tratando coluna de data_vencimento
    boletos["data_vencimento"] = pd.to_datetime(
        boletos["data_vencimento"], errors="coerce"
    ).dt.date

    # Calculando dias vencidos
    hoje = datetime.today().date()
    boletos["dias_vencidos"] = (
        (boletos["data_vencimento"] - hoje)
        .apply(lambda x: x.days) * (-1)
    )

    boletos.loc[
        boletos["status_titulo"] != "VENCIDO", "dias_vencidos"
    ] = 0

    def tratar_cnpj(cnpj):
        cnpj_limpo = "".join(filter(str.isdigit, cnpj))
        return cnpj_limpo.zfill(14)

    boletos["cnpj_sacado"] = boletos["cnpj_sacado"].apply(tratar_cnpj)
    boletos["cnpj_cedente"] = boletos["cnpj_cedente"].apply(tratar_cnpj)

    boletos_limites = boletos.merge(
        limites, on="cnpj_sacado", how="left"
    )

    boletos_limites["limite_atribuido"] = (
        boletos_limites["limite_atribuido"].fillna(0)
    )
    boletos_limites["limite_disponivel"] = (
        boletos_limites["limite_disponivel"].fillna(0)
    )

    vencer_limites = boletos_limites[
        (boletos_limites["dias_vencidos"] >= 30)
        & (boletos_limites["limite_atribuido"] > 0)
    ][[
        "cnpj_sacado",
        "cnpj_cedente",
        "nome_cedente",
        "nome_fantasia_cedente",
        "nome_sacado",
        "nome_fantasia_sacado",
        "limite_atribuido",
        "pgid"
    ]].drop_duplicates().reset_index(drop=True)

    vencer_limites["volume"] = 0
    vencer_limites["limite"] = 0
    vencer_limites[
        [
            "codigo_filial",
            "uf",
            "cdb_dba",
            "vendedor_alpe",
            "vendedor_fn_nome",
            "vendedor_fn_email",
            "vendedor_fn_telefone"
        ]
    ] = ""
    vencer_limites["prioridade"] = 6
    vencer_limites["policy"] = "V3"
    vencer_limites["pre_filtro"] = "Não"
    vencer_limites["variavel_coringa"] = "PULAR FILTROS"
    vencer_limites["bucket_pgid"] = (
        "urn-party-pgid-" + vencer_limites["pgid"]
    )

    exporta_csv = vencer_limites[[
        "cnpj_sacado",
        "volume",
        "limite",
        "nome_sacado",
        "cnpj_cedente",
        "codigo_filial",
        "uf",
        "cdb_dba",
        "vendedor_alpe",
        "vendedor_fn_nome",
        "vendedor_fn_email",
        "vendedor_fn_telefone",
        "prioridade",
        "policy",
        "pre_filtro",
        "variavel_coringa",
        "bucket_pgid"
    ]]

    vencer_limites["MOTIVO DO CANCELAMENTO"] = "VENCIDO >= 30 DIAS"

    cancelamento_v3 = (
        vencer_limites
        .groupby("MOTIVO DO CANCELAMENTO")
        .agg(
            QUANTIDADE=("cnpj_sacado", "count"),
            LIMITE_ZERADO=("limite_atribuido", "sum")
        )
        .reset_index()
    )

    cancelamento_v3["LIMITE_ZERADO"] = cancelamento_v3[
        "LIMITE_ZERADO"
    ].apply(
        lambda x: f"R$ {x:,.2f}".replace(",", "X")
        .replace(".", ",")
        .replace("X", ".")
        if pd.notnull(x) and isinstance(x, (int, float))
        else "R$ 0,00"
    )

    if cancelamento_v3.empty:
        markdown = "⚠️ Propostas V3 - Nenhum limite zerado"
    else:
        tabela_formatada = tabulate(
            cancelamento_v3.values.tolist(),
            headers=cancelamento_v3.columns.tolist(),
            tablefmt="pretty"
        )

        invisible_space = "\u200B"

        markdown = (
            "📊 Propostas V3 - Relatório semanal de Cancelamento de Limite\n\n"
            "```\n" + tabela_formatada + "\n```\n"
            f"{invisible_space}\n"
        )

    print(markdown)

    def enviar_para_webhook(mensagem):
        webhook_url = "https://yandehbr.webhook.office.com/webhookb2/aff1add1-1e5e-445d-9644-f7d9ab677641@fe284b6f-c6d2-4028-badb-7d0c22aef0ae/IncomingWebhook/cd64a656b86b4db6a8a64a74153a8555/e3ad1a1a-7716-40ee-ab81-0f05650df5dc/V2M-crEG-kOlO8wQffCBWAHSBeR29YtNktVPx1gvoiR4M1"
        headers = {"Content-Type": "application/json"}
        payload = {"text": mensagem}

        response = requests.post(
            webhook_url, json=payload, headers=headers
        )

        if response.status_code == 200:
            print("Mensagem enviada com sucesso para o Teams!")
        else:
            print(
                f"Falha ao enviar a mensagem. "
                f"Código: {response.status_code}"
            )

    enviar_para_webhook(markdown)

    print(
        f"Quantidade de CNPJs para criar issue no Jira politica v3: "
        f"{exporta_csv.shape[0]}"
    )

    exporta_csv_pgid = exporta_csv.groupby("bucket_pgid")

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

        # If the connection was successful, print the buckets
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
            grupo = grupo.drop("bucket_pgid", axis=1)

            # Gerar o arquivo CSV em memória
            csv_buffer = BytesIO()
            grupo.to_csv(
                csv_buffer,
                sep=";",
                index=False,
                encoding="utf-8",
                header=False
            )
            csv_buffer.seek(0)

            # Nome do arquivo e caminho
            object_name = (
                f"politica-credito/direcionamento-analise/in/"
                f"{bucket_pgid}.csv"
            )

            # Carregar o arquivo para o MinIO no bucket correto
            minio_client.put_object(
                bucket_name=bucket_pgid,
                object_name=object_name,
                data=csv_buffer,
                length=csv_buffer.getbuffer().nbytes,
                content_type="text/csv"
            )

            print(
                f"Arquivo {object_name} exportado para o bucket "
                f"{bucket_pgid} com {len(grupo)} linhas."
            )

        # Verificação após exportação
        if buckets_inexistentes:
            print(
                "Os seguintes buckets não existem no MinIO. "
                "Processo será interrompido:"
            )
            for bucket in buckets_inexistentes:
                print(f"- {bucket}")

            # Interrompe o processo
            raise Exception("Processo interrompido!")

    except Exception as e:
        print(f"{e}")
        exit(1)