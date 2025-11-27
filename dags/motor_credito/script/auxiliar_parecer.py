# Importando bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, concat, lpad, substring, coalesce, col, to_date, when, trim, regexp_extract, input_file_name
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import IntegerType
from pyspark.sql import Window
from minio import Minio
from io import BytesIO
from datetime import datetime, timezone, timedelta
import re
import os
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import numpy as np
from pyspark.sql.functions import split, when, array, array_union, explode, trim, first, max as spark_max, hash, col, substring, udf
from delta.tables import DeltaTable
import threading
import time
import hashlib


def auxiliar_parecer(spark, **kwargs):

    spark.sparkContext.setLogLevel("ERROR")

    # Configurações do Hadoop para acesso ao MinIO (S3 compatível)
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_TRUSTED_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_TRUSTED_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_TRUSTED_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.attempts.maximum", "1")
    hadoop_conf.set("fs.s3a.connection.establish.timeout", "10000")
    hadoop_conf.set("fs.s3a.connection.timeout", "20000")
    hadoop_conf.set("hadoop.security.authentication", "simple")
    hadoop_conf.set("hadoop.security.authorization", "false")

    # Importando bases
    print("Importando base MiniO")
    # Configurações Trusted
    trusted_socios = spark.read.format("delta").load("s3a://bureaus/receita-federal/socios")

    # Tirando configs da trusted
    hadoop_conf.unset("fs.s3a.access.key")
    hadoop_conf.unset("fs.s3a.secret.key")
    hadoop_conf.unset("fs.s3a.endpoint")

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_REFINED_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_REFINED_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_REFINED_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.attempts.maximum", "1")
    hadoop_conf.set("fs.s3a.connection.establish.timeout", "10000")
    hadoop_conf.set("fs.s3a.connection.timeout", "20000")
    hadoop_conf.set("hadoop.security.authentication", "simple")
    hadoop_conf.set("hadoop.security.authorization", "false")

    dados_cadastrais = spark.read.format("delta").load("s3a://receita-federal/dados-cadastrais")
    print("Base MiniO importada com sucesso!!")

    print("-------------------------------------------------------------------------------------")

    # Lendo dados
    # Registrar DataFrames como tabelas temporárias
    print("Criando Temporária SQL")
    trusted_socios.createOrReplaceTempView("socios")
    print("trusted_socios ok")
    dados_cadastrais.createOrReplaceTempView("dados_cadastrais")
    print("dados_cadastrais ok")

    
    print("Temporárias criadas")

    print("-------------------------------------------------------------------------------------")

    print("Cruzando df's Spark")
    # Executar consulta SQL usando as tabelas temporárias
    sql_query = """
    WITH
        dados_cadastrais AS (
            SELECT
                cnpj_sem_formatacao,
                cnpj_raiz,
                cnpj_formatado,
                data_fundacao,
                municipio,
                uf,
                cnae_principal_descricao,
                capital_social_empresa,
                data_ref,
                flag_matriz
            FROM 
                dados_cadastrais 
        ),
        socios_base AS (
            SELECT 
                cnpj_raiz,
                identificador_socio,
                `nome/razao_social` AS nome_socio,
                documento_socio,
                data_entrada_sociedade,
                ROW_NUMBER() OVER (
				    PARTITION BY cnpj_raiz 
				    ORDER BY data_entrada_sociedade ASC, documento_socio ASC
				) AS rn_asc,
                ROW_NUMBER() OVER (
				    PARTITION BY cnpj_raiz 
				    ORDER BY data_entrada_sociedade DESC, documento_socio DESC
				) AS rn_desc,
                1 as qtde_socios
            FROM socios
        ),
        socio_mais_antigo AS (
            SELECT 
                cnpj_raiz,
                nome_socio AS nome_socio_mais_antigo,
                identificador_socio AS identificador_socio_mais_antigo,
                documento_socio AS documento_socio_mais_antigo,
                data_entrada_sociedade AS data_entrada_sociedade_socio_mais_antigo
            FROM socios_base
            WHERE rn_asc = 1
        ),
        socio_mais_recente AS (
            SELECT 
                cnpj_raiz,
                nome_socio AS nome_socio_mais_recente,
                identificador_socio AS identificador_socio_mais_recente,
                documento_socio AS documento_socio_mais_recente,
                data_entrada_sociedade AS data_entrada_sociedade_socio_mais_recente
            FROM socios_base
            WHERE rn_desc = 1
        ),
        quantidade_socios as (
        	select
        		cnpj_raiz,
        		sum(qtde_socios) as qtde_socios
        	from socios_base
        	group by cnpj_raiz
        ),
        socios as (
        SELECT 
            a.cnpj_raiz,
            r.nome_socio_mais_recente,
            r.identificador_socio_mais_recente,
            r.documento_socio_mais_recente,
            r.data_entrada_sociedade_socio_mais_recente,
            a.nome_socio_mais_antigo,
            a.identificador_socio_mais_antigo,
            a.documento_socio_mais_antigo,
            a.data_entrada_sociedade_socio_mais_antigo,
            q.qtde_socios as qtde_socios
        FROM socio_mais_antigo a
        JOIN socio_mais_recente r ON a.cnpj_raiz = r.cnpj_raiz
        join quantidade_socios q on a.cnpj_raiz = q.cnpj_raiz)
        SELECT
            dc.cnpj_sem_formatacao,
            dc.cnpj_raiz,
            dc.cnpj_formatado,
            dc.municipio,
            dc.data_fundacao,
            dc.uf,
            dc.cnae_principal_descricao,
            dc.capital_social_empresa,
            dc.data_ref,
            s.nome_socio_mais_recente,
            s.identificador_socio_mais_recente,
            s.documento_socio_mais_recente,
            s.data_entrada_sociedade_socio_mais_recente,
            s.nome_socio_mais_antigo,
            s.identificador_socio_mais_antigo,
            s.documento_socio_mais_antigo,
            s.data_entrada_sociedade_socio_mais_antigo,
            s.qtde_socios
        from
            dados_cadastrais dc 
            left join socios s on dc.cnpj_raiz = s.cnpj_raiz
        where
            dc.flag_matriz = 'Sim'
    """

    # Executar a consulta SQL
    resultado_tratamento = spark.sql(sql_query)
    print("Cruzamento realizado")

    print("-------------------------------------------------------------------------------------")

    # Puxando Trino
    print("Importando dados Trino")
    conn = connect(
        host='trino.alpe.com.br',
        port=443,
        user='felipe_ferraz',
        auth=BasicAuthentication('felipe_ferraz','QiKXfAM3y<wBgCdM)]Dj'),
        http_scheme="https",
    )
    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        return pd.DataFrame(rows, columns=columns)


    # Base Boletos
    query_faturamento_arcelor = """
        with faturamento as (
        select 
            raiz_cnpj, max(vop_2023) as vop_total_2023, max(vop_2024) as vop_total_2024, max(vop_2025) as vop_total_2025, max(vop_2026) as vop_total_2026, max(max_vop_total) as maior_vop_periodo 
        from 
            deltalakerefined.payments.faturamento_externo_arcelor 
        group by 
            raiz_cnpj 
        ),
        liquidez as (
        select 
            *, ((vlr_recebido + vlr_receb_atrasado) / (vlr_recebido + vlr_receb_atrasado + vlr_inad_corrente)) as liquidez
        from(
        select raiz_cnpj, sum(vlr_recebido) as vlr_recebido, sum(vlr_receb_atrasado) as vlr_receb_atrasado, sum(vlr_inad_corrente) as vlr_inad_corrente
        from deltalaketrusted.payments.fat_pag_join 
        group by raiz_cnpj)
        where vlr_recebido <> 0)
        select 
            f.raiz_cnpj as cnpj_raiz, f.vop_total_2023, f.vop_total_2024, f.vop_total_2025, f.vop_total_2026, f.maior_vop_periodo, l.liquidez
        from 
            faturamento f
            left join liquidez l on f.raiz_cnpj = l.raiz_cnpj
    """
    faturamento_arcelor = execute_query(conn, query_faturamento_arcelor)
    print(f"Quantidade de linhas no DataFrame 'faturamento': {faturamento_arcelor.shape[0]}")
    faturamento_arcelor = spark.createDataFrame(faturamento_arcelor)
    print("Trino finalizado com sucesso!!")


    print("-------------------------------------------------------------------------------------")

    # Iniciando tratamentos
    print("Iniciando tratamentos")

    df_final = resultado_tratamento.join(faturamento_arcelor, on='cnpj_raiz', how='left')


    # Campos Parecer Faturamento Alpe

    query_faturamento_alpe = """
                        WITH base AS (
                            SELECT
                                substring(regexp_replace(cnpj_sacado, '\.', ''), 1, 8) AS cnpj8,
                                date_diff('day', data_efetivacao, data_vencimento) AS prazo,
                                valor_face
                            FROM deltalaketrusted.payments.boletos_internos
                        ),

                        prazo_arredondado AS (
                            SELECT
                                cnpj8,
                                valor_face,
                                case WHEN prazo < 60 then 15 * ceil(prazo / 15.0) else 30 * ceil(prazo / 30.0) END AS faixa_prazo
                            FROM base
                        ),

                        somas AS (
                            SELECT
                                cnpj8,
                                faixa_prazo,
                                sum(valor_face) AS soma_faixa
                            FROM prazo_arredondado
                            GROUP BY 1, 2
                        ),

                        totais AS (
                            SELECT
                                cnpj8,
                                sum(soma_faixa) AS total
                            FROM somas
                            GROUP BY 1
                        ),

                        percentual AS (
                            SELECT
                                s.cnpj8,
                                s.faixa_prazo,
                                s.soma_faixa,
                                (s.soma_faixa / t.total) AS pct
                            FROM somas s
                            JOIN totais t
                            ON s.cnpj8 = t.cnpj8
                        ),

                        faixas_relevantes AS (
                            SELECT
                                cnpj8,
                                faixa_prazo
                            FROM percentual
                            WHERE pct >= 0.10  -- SOMENTE FAIXAS COM MAIS DE 10%
                        ),

                        final AS (
                            SELECT
                                cnpj8,
                                array_join(array_sort(array_agg(DISTINCT faixa_prazo)),'-') AS fluxo_pagamento
                            FROM faixas_relevantes
                            GROUP BY 1
                        ),

                        max_atraso as (
                            select
                                substring(regexp_replace(cnpj_sacado, '[./-]', ''), 1, 8) as cnpj_raiz,
                                max(dias_em_atraso) as max_atraso
                            from deltalakerefined.payments.carteira_vendermais cv
                            group by 1
                        ),

                        max_vop_mensal as (
                            select
                                sub1.cnpj_raiz,
                                max(sub1.vop) as max_vop_mensal
                            from (
                                select
                                    substring(regexp_replace(cnpj_sacado, '[./-]', ''), 1, 8) as cnpj_raiz,
                                    safra_concessao,
                                    sum(valor_face) as vop
                                from deltalaketrusted.payments.boletos_internos
                                group by 1, 2 ) as sub1
                            group by 1
                        ),


                        liquidez_interna as (
                            select
                                sub2.raiz_cnpj_sacado as cnpj_raiz,
                                sub2.pago,
                                sub2.pago + sub2.pago_em_atraso as pago_total,
                                (sub2.pago / NULLIF(sub2.pago + sub2.pago_em_atraso, 0)) * 100 AS liq_interna
                            from (
                                    select
                                    sub.raiz_cnpj_sacado,
                                    sum(case when sub.status_pagamento_custom = 'A VENCER' then sub.vop else 0 end) as vop_a_vencer,
                                    sum(case when sub.status_pagamento_custom = 'PAGO' then sub.vop else 0 end) as pago,
                                    sum(case when sub.status_pagamento_custom = 'PAGO EM ATRASO' then sub.vop else 0 end) as pago_em_atraso,
                                    sum(case when sub.status_pagamento_custom = 'VENCIDO' then sub.vop else 0 end) as vop_vencido
                                    from (
                                    select
                                        substring(replace(replace(replace(cnpj_sacado, '.', ''), '/', ''), '-', ''), 1, 8) as raiz_cnpj_sacado,
                                        data_baixa,
                                        data_vencimento,
                                        data_emissao,
                                        valor_face as vop,
                                        case
                                        when data_baixa is not null then
                                            case
                                            when data_baixa <= date_add('day', 10, data_vencimento) then 'PAGO'
                                            else 'PAGO EM ATRASO'
                                            end
                                        else
                                            case
                                            when current_date > data_vencimento then 'VENCIDO'
                                            else 'A VENCER'
                                            end
                                        end as status_pagamento_custom
                                    from deltalaketrusted.payments.boletos_internos
                                    ) as sub
                                    group by sub.raiz_cnpj_sacado ) as sub2
                        ),

                        infos_vop as (
                            select
                                substring ( cnpj_sacado, 1, 8 ) as cnpj_raiz,
                                sum ( vop ) as vop_total,
                                sum ( vop_performado ) as vop_performado,
                                sum ( vop_a_vencer ) as vop_a_vencer,
                                avg ( prazo_medio ) as prazo_medio,
                                min ( safra_concessao ) as safra_concessao_min,
                                array_join(array_agg(distinct nome_cedente), ', ') as nomes_cedentes
                            from deltalakerefined.payments.vop_vendermais
                            group by 1
                        )

                        select
                            iv.*,
                            f.fluxo_pagamento,
                            ma.max_atraso,
                            mvm.max_vop_mensal,
                            li.liq_interna
                        FROM infos_vop iv
                        left join final f
                            on iv.cnpj_raiz = f.cnpj8
                        left join max_atraso ma 
                            on iv.cnpj_raiz = ma.cnpj_raiz
                        left join max_vop_mensal mvm
                            on iv.cnpj_raiz = mvm.cnpj_raiz
                        left join liquidez_interna li
                            on iv.cnpj_raiz = li.cnpj_raiz
    """

    faturamento_alpe = execute_query(conn, query_faturamento_alpe)
    print(f"Quantidade de linhas no DataFrame 'faturamento alpe': {faturamento_alpe.shape[0]}")
    faturamento_alpe = spark.createDataFrame(faturamento_alpe)
    print("Trino finalizado com sucesso!!")


    faturamento_alpe = (
        faturamento_alpe

            .withColumn("vop_total", F.round(F.col("vop_total"), 2))
            .withColumn("vop_performado", F.round(F.col("vop_performado"), 2))
            .withColumn("vop_a_vencer", F.round(F.col("vop_a_vencer"), 2))
            .withColumn("max_vop_mensal", F.round(F.col("max_vop_mensal"), 2))

            .withColumn("prazo_medio", F.round(F.col("prazo_medio"), 0))
            .withColumn("max_atraso", F.round(F.col("max_atraso"), 0))
            .withColumn("liq_interna", F.round(F.col("liq_interna"), 0))

            .withColumn(
                "safra_concessao_min",
                F.date_format(F.col("safra_concessao_min"), "dd/MM/yyyy")
            )
    )


    print("Iniciando cruzamento")

    df_final = df_final.join(faturamento_alpe, on='cnpj_raiz', how='left')


    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    atualizado_em = now.strftime('%Y-%m-%d %X')  
    df_final = df_final.withColumn("atualizado_em", lit(atualizado_em))

    def stable_hash_bucket(cnpj_raiz: str, num_buckets: int = 200) -> int:
        if not cnpj_raiz:
            return None
        try:
            cnpj_str = str(cnpj_raiz)
            cnpj_clean = ''.join([c for c in cnpj_str if c.isdigit()])[:8]
            if len(cnpj_clean) < 8:
                return None
            hash_int = int(hashlib.md5(cnpj_clean.encode()).hexdigest(), 16)
            return hash_int % num_buckets
        except Exception as e:
            return None

    bucket_udf = udf(lambda cnpj: stable_hash_bucket(cnpj), IntegerType())

    df_final = df_final.withColumn(
        "cnpj_bucket", bucket_udf(col("cnpj_raiz"))
    )

    print("Tratamentos Finalizados")


    print("-------------------------------------------------------------------------------------")

    print('Salvando arquivo...')

    def keep_alive_logger(interval=60):
        """Thread para imprimir mensagens de keep-alive."""
        while not done_flag.is_set():
            print("[INFO] Processando escrita no Delta... ainda rodando.")
            time.sleep(interval)

    done_flag = threading.Event()
    logger_thread = threading.Thread(target=keep_alive_logger)
    logger_thread.start()


    # # Tirando configs
    # hadoop_conf.unset("fs.s3a.access.key")
    # hadoop_conf.unset("fs.s3a.secret.key")
    # hadoop_conf.unset("fs.s3a.endpoint")

    # # Salvando arquivos
    # hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_REFINED_ACCESS_KEY'))
    # hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_REFINED_SECRET_KEY'))
    # hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_REFINED_ENDPOINT'))
    # hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    # hadoop_conf.set("fs.s3a.path.style.access", "true")

    try:
        df_final.write \
            .partitionBy("data_ref", "cnpj_bucket") \
            .format("delta") \
            .option("overwriteSchema", "true") \
            .mode("overwrite") \
            .save("s3a://motor/auxiliar_parecer")

        print("[INFO] Arquivos Salvos com sucesso.")  
    finally:
        done_flag.set()
        logger_thread.join()


    spark.stop()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("AuxiliarParecer") \
        .config("spark.sql.encoding", "latin1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "100") \
        .config("spark.shuffle.compress", "true") \
        .config("spark.shuffle.spill.compress", "true") \
        .config("spark.shuffle.file.buffer", "64k") \
        .config("spark.local.dir", "/tmp/spark") \
        .config("spark.driver.extraJavaOptions", "-Divy.cache.dir=/tmp -Divy.home=/tmp") \
        .config("spark.executor.extraJavaOptions", "-Divy.cache.dir=/tmp -Divy.home=/tmp") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    auxiliar_parecer(spark)