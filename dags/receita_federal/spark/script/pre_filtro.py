# Importando bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, concat, lpad, substring, coalesce, col, to_date, when, trim, regexp_extract, input_file_name
from pyspark.sql.functions import *
from pyspark.sql.types import *
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
from pyspark.sql.functions import col, split, when, array, array_union, explode, trim, first, max as spark_max, hash, col
from delta.tables import DeltaTable
import threading
import time


def cnaes_to_trusted(spark, **kwargs):

    spark.sparkContext.setLogLevel("ERROR")

    # Configurações do Hadoop para acesso ao MinIO (S3 compatível)
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", 'nr0qPLaAcdCtt7lAV4oa') 
    hadoop_conf.set("fs.s3a.secret.key", 'GRA8FxnVMy7pGDvKP1wZK2nPOC3vP7F1AvH2u3Ch') 
    hadoop_conf.set("fs.s3a.endpoint", 'api-trusted.alpe.com.br') 
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.attempts.maximum", "1")
    hadoop_conf.set("fs.s3a.connection.establish.timeout", "10000")
    hadoop_conf.set("fs.s3a.connection.timeout", "20000")
    hadoop_conf.set("hadoop.security.authentication", "simple")
    hadoop_conf.set("hadoop.security.authorization", "false")

    # Leitura dos dados brutos da camada Trusted
    trusted_estabelecimentos = spark.read.format("delta").load("s3a://bureaus/receita-federal/estabelecimentos")
    trusted_empresas = spark.read.format("delta").load("s3a://bureaus/receita-federal/empresas")
    trusted_simples = spark.read.format("delta").load("s3a://bureaus/receita-federal/simples")
    trusted_natureza_juridica = spark.read.format("delta").load("s3a://bureaus/receita-federal/naturezas")
    trusted_pep = spark.read.format("delta").load("s3a://pessoas-e-organizacoes/pep")
    trusted_socios = spark.read.format("delta").load("s3a://bureaus/receita-federal/socios")
    trusted_cnae = spark.read.format("delta").load("s3a://bureaus/receita-federal/cnaes")

    # Lendo dados
    # Registrar DataFrames como tabelas temporárias
    trusted_socios.createOrReplaceTempView("socios")
    print("trusted_socios ok")
    trusted_pep.createOrReplaceTempView("pep")
    print("trusted_pep ok")
    trusted_estabelecimentos.createOrReplaceTempView("estabelecimentos")
    print("trusted_estabelecimentos ok")
    trusted_empresas.createOrReplaceTempView("empresas")
    print("trusted_empresas ok")
    trusted_simples.createOrReplaceTempView("simples")
    print("trusted_simples ok")
    trusted_natureza_juridica.createOrReplaceTempView("natureza_juridica")
    print("trusted_natureza_juridica ok")
    trusted_cnae.createOrReplaceTempView("cnae")
    print("trusted_cnae ok")


    print("TRINO_ENDPOINT:", os.getenv('TRINO_ENDPOINT'))
    print("TRINO_PORT:", os.getenv('TRINO_PORT', '443'))
    print("TRINO_USER:", os.getenv('TRINO_USER'))
    print("TRINO_PASSWORD:", "******" if os.getenv('TRINO_PASSWORD') else None)


    # Conexão com o Trino
    conn = connect(
        host='trino.alpe.com.br',
        port=443,
        user='vinicius_teixeira',
        auth=BasicAuthentication('vinicius_teixeira', 'TEjcv)-+b}o!QL5CM2:p'),
        http_scheme="https",
    )

    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        
        return pd.DataFrame(rows, columns=columns)


    # Base auxiliar CNAE
    query_cnae = """
        SELECT * 
        FROM miniorefined.dimensao.cnaes_aceitos
    """
    aux_cnae = execute_query(conn, query_cnae)


    # Base auxiliar Natureza Jurídica
    query_nat_ju = """
        SELECT * 
        FROM miniorefined.dimensao.natureza_juridica
    """
    aux_nat_ju = execute_query(conn, query_nat_ju)


    # Base para pegar historico de propostas ultimos 60 dias
    query_propostas= """
            SELECT 	cnpj,
                max(raiz_cnpj) as raiz_cnpj,
                true as analise_menor_60_dias,
                max(decisao) as decisao
        FROM deltalaketrusted.jira.propostas
        WHERE try_cast(data_criado AS date) >= current_date - INTERVAL '60' day
        group by cnpj
    """
    propostas_aux = execute_query(conn, query_propostas)


    query_info_limite = f"""
    select 
        cnpj_raiz as cnpj_raiz_lim, 
        case when limite_atribuido > 0 then true else false end as limite_alpe, 
        NULLIF(limite_disponivel, 0) / NULLIF(limite_atribuido, 0) AS pcto_limite_utilizado, 
        situacao_sacado, 
        limite_atribuido
    from (
        select 
            pc.chave as cnpj_raiz,
            lc.id is not null as limite_alpe,
            sum(limite_atribuido) as limite_atribuido,
            sum(limite_disponivel) as limite_disponivel,
            pl.status as situacao_sacado
        from postgres.ccred_schema_prd_default.participante_chave pc
        inner join postgres.ccred_schema_prd_default.limite_config lc on lc.participante_chave_sacado_id = pc.id
        inner join postgres.ccred_schema_prd_default.participante_limite pl on pl.limite_config_id = lc.id	    
        group by
            pc.chave, lc.id is not null, pl.status)
    """

    limite_info = execute_query(conn, query_info_limite)


    # Transformar DFSs Pandas em Spark

    aux_nat_ju_spark = spark.createDataFrame(aux_nat_ju)
    aux_cnae_spark = spark.createDataFrame(aux_cnae)
    aux_propostas_spark = spark.createDataFrame(propostas_aux)
    aux_limite_info_spark = spark.createDataFrame(limite_info)


    # Executar consulta SQL usando as tabelas temporárias
    sql_query = """
    WITH
        tem_pep AS (
            SELECT
                DISTINCT
                s.cnpj_raiz,
                TRUE AS tem_pep
            FROM pep p
            JOIN socios s ON REPLACE(REPLACE(p.documento, '.', ''), '-', '') = s.documento_socio 
                        AND p.nome = s.`nome/razao_social`
            WHERE s.data_ref = (SELECT MAX(data_ref) FROM socios)
            AND p.data_ref = (SELECT MAX(data_ref) FROM pep)
        ),
        socio AS (
            SELECT
                cnpj_raiz,
                CASE WHEN
                    MAX(CASE WHEN identificador_socio = 'PESSOA JURÍDICA' THEN 1 ELSE 0 END) = 1
                    THEN true
                    ELSE false
                    END as tem_socio_pj,
                MAX(data_entrada_sociedade) AS mais_recente,
                MAX(data_ref) AS data_ref
            FROM socios
            GROUP BY
                cnpj_raiz
        )

        

    SELECT
        est.cnpj_raiz AS cnpj_raiz,
        est.documento_sem_formatacao AS documento_sem_formatacao,
        UPPER(emp.razao_social) as razao_social,
        est.cnae_principal AS cod_cnae,
        est.cnae_secundaria,
        emp.natureza_juridica AS cod_natureza_juridica,
        (DATEDIFF(CURRENT_DATE(), TO_DATE(est.data_inicio_atividade)) / 365.00) AS idade,
        emp.codigo_porte_empresa, 
        emp.capital_social_empresa,
        est.situacao_cadastral AS situacao_cadastral,
        (DATEDIFF(CURRENT_DATE(), TO_DATE(s.mais_recente)) / 365.00) AS idade_socio,
        COALESCE(s.tem_socio_pj, FALSE) AS tem_socio_pj,
        COALESCE(sim.is_mei, FALSE) AS is_mei,
        COALESCE(tp.tem_pep, FALSE) AS tem_pep,
        COALESCE(est.is_matriz, FALSE) AS is_matriz,
        est.situacao_especial,
        est.data_ref AS data_ref_receita
    FROM estabelecimentos est
    LEFT JOIN empresas emp ON emp.cnpj_raiz = est.cnpj_raiz AND est.data_ref = emp.data_ref
    LEFT JOIN simples sim ON sim.cnpj_raiz = est.cnpj_raiz AND est.data_ref = sim.data_ref
    LEFT JOIN natureza_juridica natjur ON natjur.codigo = emp.natureza_juridica AND est.data_ref = natjur.data_ref
    LEFT JOIN cnae ON cnae.codigo = est.cnae_principal AND est.data_ref = cnae.data_ref
    LEFT JOIN tem_pep tp ON tp.cnpj_raiz = est.cnpj_raiz
    LEFT JOIN socio s ON est.cnpj_raiz = s.cnpj_raiz AND est.data_ref = s.data_ref
    """

    # Executar a consulta SQL
    resultado_tratamento = spark.sql(sql_query)


    # Une cnae principal com todos os secundarios
    resultado_tratamento = resultado_tratamento.withColumn(
        "todos_cnaes",
        array_union(
            array(trim(col("cod_cnae"))),
            when(col("cnae_secundaria").isNotNull(), split(col("cnae_secundaria"), ","))
            .otherwise(array())
        )
    )

    # Criando uma linha para cada cnae, afim de analisar se pelo menos um cnae é aceito
    resultado_explodido = resultado_tratamento.withColumn(
        "cnae_temp", explode(col("todos_cnaes"))
    )

    # Alterando nome da coluna para não dar interferencia no join
    resultado_explodido = resultado_explodido.withColumn(
        "cnae", trim(col("cnae_temp"))
    )


    df_validado = resultado_explodido.join(
        aux_cnae_spark,
        resultado_explodido["cnae"] == aux_cnae_spark["cod_cnae"],
        how="left"
    )

    df_validado = df_validado.drop(aux_cnae_spark["cod_cnae"])


    # Criando coluna auxiliar para a flag de aceito ou não
    df_validado = df_validado.withColumn(
        "cnae_aceito_flag", when(col("cnae_aceito") == "SIM", 1).otherwise(0)
    )

    # Agrupando novamente
    df_agrupado = df_validado.groupBy("documento_sem_formatacao").agg(
        first("cnpj_raiz").alias("cnpj_raiz"),
        first("razao_social").alias("razao_social"),
        first("cod_cnae").alias("cod_cnae"),
        first("cnae_secundaria").alias("cnae_secundaria"),
        first("cod_natureza_juridica").alias("cod_natureza_juridica"),
        first("idade").alias("idade"),
        first("codigo_porte_empresa").alias("codigo_porte_empresa"),
        first("capital_social_empresa").alias("capital_social_empresa"),
        first("situacao_cadastral").alias("situacao_cadastral"),
        first("idade_socio").alias("idade_socio"),
        first("tem_socio_pj").alias("tem_socio_pj"),
        first("is_mei").alias("is_mei"),
        first("tem_pep").alias("tem_pep"),
        first("is_matriz").alias("is_matriz"),
        first("situacao_especial").alias("situacao_especial"),
        first("data_ref_receita").alias("data_ref_receita"),
        when(spark_max("cnae_aceito_flag") == 1, "SIM").otherwise("NAO").alias("cnae_aceito")
    )


    ## Cruzando com informações de Natureza Juridica

    # Renomeia a chave da tabela auxiliar
    aux_nat_ju_spark = aux_nat_ju_spark.withColumnRenamed("cod_natureza_juridica", "cod_nat_aux")

    # Faz o join com a chave renomeada
    df_agrupado = df_agrupado.join(
        aux_nat_ju_spark,
        df_agrupado["cod_natureza_juridica"] == aux_nat_ju_spark["cod_nat_aux"],
        how="left"
    )

    # IS SPE
    df_agrupado = df_agrupado.withColumn(
        "is_spe",
        (col("cod_natureza_juridica") == "2062") &
        (
            col("razao_social").startswith("SPE ") |
            col("razao_social").endswith(" SPE") | 
            col("razao_social").endswith(" SPE LTDA")
        )
    )

    # IS SA
    df_agrupado = df_agrupado.withColumn(
        "is_sa",
        (col("cod_natureza_juridica") == "2054") |
        (col("cod_natureza_juridica") == "2046")
    )

    # IS CONSORCIO
    df_agrupado = df_agrupado.withColumn(
        "is_consorcio",
        (col("cod_natureza_juridica").isin("1210", "1228", "2151", "2283", "2291")) &
        (
            col("razao_social").startswith("CONSORCIO ") |
            col("razao_social").endswith(" CONSORCIO")
        )
    )

    # IS CONSTRUTURA
    df_agrupado = df_agrupado.withColumn(
        "is_construtora",
        (col("cod_natureza_juridica").isin("2062", "2135")) &
        (col("cod_cnae").isin(
            "3011301", "3011302", "3012100", "4120400", "4211101", "4212000",
            "4221901", "4221902", "4221904", "4222701", "4223500", "4299501"
        ))
    )


    # Cruzando com informações de propostas
    df_agrupado = df_agrupado.join(
        aux_propostas_spark,
        df_agrupado["documento_sem_formatacao"] == aux_propostas_spark["cnpj"],
        how="left"
    )

    # Cruzando com informações de limite
    df_agrupado = df_agrupado.join(
        aux_limite_info_spark,
        df_agrupado["cnpj_raiz"] == aux_limite_info_spark["cnpj_raiz_lim"],
        how="left"
    )


    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    atualizado_em = now.strftime('%Y-%m-%d %X')  
    df_agrupado = df_agrupado.withColumn("atualizado_em", lit(atualizado_em))


    colunas_finais = [
    'documento_sem_formatacao','cnpj_raiz','razao_social','cod_cnae','cnae_secundaria',
    'cod_natureza_juridica','idade','codigo_porte_empresa','capital_social_empresa',
    'situacao_cadastral','situacao_sacado', 'limite_alpe', 'limite_atribuido', 'pcto_limite_utilizado',
    'idade_socio','tem_socio_pj','is_mei','is_matriz', 'is_spe','is_consorcio',
    'is_construtora','tem_pep','situacao_especial','data_ref_receita','cnae_aceito',
    'nat_ju_aceita','analise_menor_60_dias','decisao','atualizado_em'
    ]

    df_filtrado = df_agrupado.select(colunas_finais)

    # Filtrando apenas oq for is matriz True para salvar no Minio
    df_final = df_filtrado.filter(df_filtrado["is_matriz"] == True)

    #print(f"DataFrame carregado. Esquema: {df_final.printSchema()}")
    #print(f"Primeiras linhas do DataFrame: {df_final.show(5)}") 
    #row_count = df_final.count()
    #print(f"Número de linhas no DataFrame: {row_count}")

    print('Salvando arquivo...')

    # Tirando configs da trusted
    hadoop_conf.unset("fs.s3a.access.key")
    hadoop_conf.unset("fs.s3a.secret.key")
    hadoop_conf.unset("fs.s3a.endpoint")

    # Salvando arquivos
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_REFINED_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_REFINED_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_REFINED_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")


    df_final = df_final.withColumn(
        "cnpj_bucket", (hash(col("documento_sem_formatacao")) % 100).cast("int")
    )


    def keep_alive_logger(interval=60):
        """Thread para imprimir mensagens de keep-alive."""
        while not done_flag.is_set():
            print("[INFO] Processando escrita no Delta... ainda rodando.")
            time.sleep(interval)

    done_flag = threading.Event()
    logger_thread = threading.Thread(target=keep_alive_logger)
    logger_thread.start()


    try:
        df_final.write \
            .partitionBy("data_ref_receita", "cnpj_bucket") \
            .format("delta") \
            .option("overwriteSchema", "true") \
            .mode("overwrite") \
            .save("s3a://motor/pre_filtro_v2")

        print("[INFO] Arquivos Salvos com sucesso.")  
    finally:
        done_flag.set()
        logger_thread.join()


    spark.stop()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("PrefiltroToRefined") \
        .config("spark.sql.encoding", "latin1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    cnaes_to_trusted(spark)
