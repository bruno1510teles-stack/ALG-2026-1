from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when, to_date, date_format, current_timestamp
from minio import Minio
import os
from datetime import datetime, timezone, timedelta


def dados_cadastrais_to_refined(spark):

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
    # Leitura dos dados brutos da camada Trusted e tratando colunas
    estabelecimentos = spark.read.format("delta").load("s3a://teste-felipe/receita-federal/estabelecimentos")\
            .withColumn("data_situacao_cadastral", col("data_situacao_cadastral").cast("date"))
    empresas = spark.read.format("delta").load("s3a://teste-felipe/receita-federal/empresas")\
            .withColumn("capital_social_empresa", col("capital_social_empresa").cast("double"))
    simples = spark.read.format("delta").load("s3a://teste-felipe/receita-federal/simples")
    natureza = spark.read.format("delta").load("s3a://teste-felipe/receita-federal/naturezas")
    cnae = spark.read.format("delta").load("s3a://teste-felipe/receita-federal/cnaes")
    municipio = spark.read.format("delta").load("s3a://teste-felipe/receita-federal/municipios")
    print("Arquivos lidos")
    

     #Criação da tabela final
    dados_cadastrais = (
        estabelecimentos.alias("e")
        .join(empresas.alias("emp"), col("e.cnpj_raiz") == col("emp.cnpj_raiz"), "left")
        .join(simples.alias("s"), col("e.cnpj_raiz") == col("s.cnpj_raiz"), "left")
        .join(cnae.alias("cnae"), col("e.cnae_principal") == col("cnae.codigo"), "left")
        .join(natureza.alias("nat"), col("emp.natureza_juridica") == col("nat.codigo"), "left")
        .join(municipio.alias("m"), col("e.municipio") == col("m.codigo"), "left")
        .select(
            col("e.cnpj_raiz"),
            col("e.documento_sem_formatacao").alias("cnpj_sem_formatacao"),
            col("e.documento_formatado").alias("cnpj_formatado"),
            when(col("e.is_matriz") == True, "Sim").otherwise("Nao").alias("flag_matriz"),
            col("emp.razao_social"),
            col("e.nome_fantasia"),
            col("e.situacao_cadastral"),
            to_date(col("e.data_situacao_cadastral").cast("date"), "yyyy-MM-dd").alias("data_situacao_cadastral"),
            to_date(col("e.data_inicio_atividade"), "yyyy-MM-dd").alias("data_fundacao"),
            col("e.cnae_principal").alias("cnae_principal_codigo"),
            col("cnae.descricao").alias("cnae_principal_descricao"),
            col("e.cnae_secundaria"),
            col("e.logradouro"), 
            col("e.numero"), 
            col("e.complemento"), 
            col("e.bairro"), 
            col("e.cep"),
            col("e.uf"), 
            col("m.descricao").alias("municipio"),
            col("e.situacao_especial"), 
            to_date(col("e.data_sitaucao_especial"), "yyyy-MM-dd").alias("data_situacao_especial"),
            col("emp.natureza_juridica").alias("natureza_juridica_codigo"),
            col("nat.descricao").alias("descricao_natureza_juridica"),
            col("emp.capital_social_empresa").cast("double").alias("capital_social_empresa"),
            col("emp.porte_empresa"),
            when(col("s.is_simples") == True, "Sim").otherwise("Nao").alias("flag_simples"),
            to_date(col("s.data_opcao_pelo_simples"), "yyyy-MM-dd").alias("data_opcao_simples"),
            to_date(col("s.data_exclusao_simples"), "yyyy-MM-dd").alias("data_exclusao_simples"),
            when(col("s.is_mei") == True, "Sim").otherwise("Nao").alias("flag_mei"),
            to_date(col("s.data_opcao_pelo_mei"), "yyyy-MM-dd").alias("data_opcao_mei"),
            to_date(col("s.data_exclusao_mei"), "yyyy-MM-dd").alias("data_exclusao_mei"),
            col("e.data_ref"),
            date_format(current_timestamp(), "yyyy-MM-dd").alias("Atualizado_em")
            

        )
    )
    

    # Salvando arquivos
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_REFINED_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_REFINED_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_REFINED_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")

    print("Iniciando salvamento dos arquivos")
    dados_cadastrais.write \
        .partitionBy("data_ref") \
        .format("delta") \
        .option("mergeSchema", "true") \
        .option("encoding", 'latin1') \
        .mode("overwrite") \
        .save(f"s3a://receita-federal/dados-cadastrais") 

    print("Arquivos Salvos")

    spark.stop()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("PrefiltroToRefined") \
        .config("spark.sql.encoding", "latin1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite","LEGACY") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    dados_cadastrais_to_refined(spark)
