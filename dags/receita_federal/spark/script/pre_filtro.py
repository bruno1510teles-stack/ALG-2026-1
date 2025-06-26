from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from minio import Minio
import os
from datetime import datetime, timezone, timedelta


def cnaes_to_trusted(spark):

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_TRUSTED_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_TRUSTED_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_TRUSTED_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
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
        emp.razao_social,
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


    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    atualizado_em = now.strftime('%Y-%m-%d %X')  
    resultado_tratamento = resultado_tratamento.withColumn("atualizado_em", lit(atualizado_em))
    # Mostrar o resultado
    resultado_tratamento.show(10)

    print(f"DataFrame carregado. Esquema: {resultado_tratamento.printSchema()}")
    print(f"Primeiras linhas do DataFrame: {resultado_tratamento.show(5)}") 
    row_count = resultado_tratamento.count()
    print(f"Número de linhas no DataFrame: {row_count}") 

    # Salvando arquivos
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_REFINED_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_REFINED_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_REFINED_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")

    print("Iniciando salvamento dos arquivos")
    resultado_tratamento.write \
        .partitionBy("data_ref_receita") \
        .format("delta") \
        .option("overwriteSchema", "true") \
        .option("encoding", 'latin1') \
        .mode("overwrite") \
        .save(f"s3a://motor/pre_filtro") 

    print("Arquivos Salvos")

    spark.stop()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("PrefiltroToRefined") \
        .config("spark.sql.encoding", "latin1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    cnaes_to_trusted(spark)
