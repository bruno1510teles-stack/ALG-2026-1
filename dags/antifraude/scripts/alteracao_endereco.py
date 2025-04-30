from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import concat, lit, col, when, input_file_name, regexp_extract
from minio import Minio
import os
from itertools import chain
from datetime import datetime, timezone, timedelta


def alteracao_endereco(spark):

    # Parametrizando conexões
    print("Iniciando conexões")
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
    print("Conexões realizadas com sucesso!!")

    # Puxando base
    print("Importando Base")
    df = spark.read.format("delta").load("s3a://bureaus/receita-federal/estabelecimentos_aud")
    df_estabelecimentos = spark.read.format("delta").load("s3a://bureaus/receita-federal/estabelecimentos")
    municipio = spark.read.format("delta").load("s3a://bureaus/receita-federal/municipios")
    print("Base Importada com sucesso!!")


    # Iniciando tratamentos
    print("Iniciando tratamentos")

    municipio = municipio.select("codigo","descricao")

    df_estabelecimentos = df_estabelecimentos.withColumn(
        "endereco_completo", 
        F.concat_ws(" - ", 
                    F.col("logradouro"), 
                    F.col("numero"),
                    F.col("cep")
                )
    )

    df_estabelecimentos = df_estabelecimentos.select("documento_sem_formatacao","endereco_completo")

    valida_endereco = df_estabelecimentos.groupBy("endereco_completo") \
        .agg(F.countDistinct("documento_sem_formatacao").alias("quantidade_cnpjs"))


    df = df.join(
        municipio,
        on = df["municipio"] == municipio["codigo"],
        how = "inner"
    )

    df = df_estabelecimentos.join(
        df,
        on = df["documento_sem_formatacao"] == df_estabelecimentos["documento_sem_formatacao"],
        how = "inner"
    ).drop(df_estabelecimentos["documento_sem_formatacao"])

    df = df.join(
        valida_endereco,
        on = df["endereco_completo"] == valida_endereco["endereco_completo"],
        how = "left"
    ).drop(valida_endereco["endereco_completo"])


    df = df.groupBy("documento_sem_formatacao", "endereco_completo") \
        .agg(
            F.collect_set("logradouro").alias("endereco_distintos"),
            F.collect_set("descricao").alias("cidade_distintos"),
            F.collect_set("uf").alias("estado_distintos"),
            F.max("data_ref").alias("data_referencia"),
            F.max("quantidade_cnpjs").alias("quantidade_cnpjs_mesmo_endereco")
        ) \
        .withColumn("flag_mudanca_endereco", F.size("endereco_distintos") > 1) \
        .withColumn("flag_mudanca_endereco", F.when(F.col("flag_mudanca_endereco") == True, "Sim").otherwise("Nao")) \
        .withColumn("flag_mudanca_cidade", F.size("cidade_distintos") > 1) \
        .withColumn("flag_mudanca_cidade", F.when(F.col("flag_mudanca_cidade") == True, "Sim").otherwise("Nao")) \
        .withColumn("flag_mudanca_estado", F.size("estado_distintos") > 1) \
        .withColumn("flag_mudanca_estado", F.when(F.col("flag_mudanca_estado") == True, "Sim").otherwise("Nao")) \
        .withColumn("flag_endereco_igual", F.col("quantidade_cnpjs_mesmo_endereco") > 1) \
        .withColumn("flag_endereco_igual", F.when(F.col("flag_endereco_igual") == True, "Sim").otherwise("Nao"))

    df = df.withColumnRenamed("documento_sem_formatacao", "cnpj_sem_formatacao")
    df = df.withColumnRenamed("endereco_completo", "endereco_completo_atual")


    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    atualizado_em = now.strftime('%Y-%m-%d %X')  
    df = df.withColumn("atualizado_em", lit(atualizado_em))

    df = df.select("cnpj_sem_formatacao", "endereco_completo_atual", "endereco_distintos", "flag_mudanca_endereco", "cidade_distintos", "flag_mudanca_cidade", "estado_distintos", "flag_mudanca_estado",	"quantidade_cnpjs_mesmo_endereco",  "flag_endereco_igual", "data_referencia" , "atualizado_em")			
    print("Tratamentos realizados com sucesso!!")
    
    df.show(5)


    # Tirando configs da trusted
    hadoop_conf.unset("fs.s3a.access.key")
    hadoop_conf.unset("fs.s3a.secret.key")
    hadoop_conf.unset("fs.s3a.endpoint")

    # Colocando configs refined
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_REFINED_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_REFINED_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_REFINED_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")

    print("Iniciando salvamento dos arquivos")
    df.write \
        .partitionBy("data_referencia") \
        .format("delta") \
        .option("mergeSchema", "true") \
        .option("encoding", 'latin1') \
        .mode("overwrite") \
        .save("s3a://refined-antifraude/alteracao_endereco")
    print("Arquivos Salvos")


    spark.stop()
    
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("antifraude_alteracao_endereco") \
        .config("spark.sql.encoding", "latin1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "2") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    alteracao_endereco(spark)