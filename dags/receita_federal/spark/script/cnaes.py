from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from minio import Minio
import os
from datetime import datetime, timezone, timedelta


def cnaes_to_trusted(spark):
    def generate_client_minio():
        parsedurl = os.getenv('MINIO_RAW_ENDPOINT')
        return Minio(
            parsedurl,
            os.getenv('MINIO_RAW_ACCESS_KEY'),
            os.getenv('MINIO_RAW_SECRET_KEY')
        )

    def get_newest_file_path(client, bucket, path): 
        objects = client.list_objects(bucket, f'{path}/')
        max_ano = max(obj.object_name for obj in objects)
        
        objects = client.list_objects(bucket, max_ano)
        max_particao = max(obj.object_name for obj in objects)
        
        return max_particao

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_RAW_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_RAW_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_RAW_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.attempts.maximum", "1")
    hadoop_conf.set("fs.s3a.connection.establish.timeout", "10000")
    hadoop_conf.set("fs.s3a.connection.timeout", "20000")
    hadoop_conf.set("hadoop.security.authentication", "simple")
    hadoop_conf.set("hadoop.security.authorization", "false")

    client = generate_client_minio()

    BUCKET_SOURCE = "receita-federal"
    PATH = "cnaes"
    
    print("Lendo pasta mais recente")
    file_path = get_newest_file_path(client, BUCKET_SOURCE, PATH)
    print("Pasta mais recente: {}".format(file_path))

    print("Lendo arquivo")
    trusted_cnae = spark.read \
        .option("delimiter", ";") \
        .option("header", False)\
        .option("encoding", 'latin1')\
        .csv(f"s3a://{BUCKET_SOURCE}/{file_path}")
    print("Arquivo lido: {}".format(file_path))

    print("Iniciando tratamento")
    trusted_cnae = trusted_cnae \
        .withColumnRenamed("_c0", "codigo") \
        .withColumnRenamed("_c1", "descricao")
        
    
    # Extraindo o ano e o mês do caminho do arquivo
    ano = file_path.split("year=")[1].split("/")[0]
    mes = file_path.split("month=")[1].split("/")[0]
    # Construindo o ANOMES no formato desejado
    data_ref = ano + mes
    # Adicionando a coluna "data_ref" ao DataFrame
    trusted_cnae = trusted_cnae.withColumn("data_ref", lit(data_ref))

    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    atualizado_em = now.strftime('%Y-%m-%d %X')  
    trusted_cnae = trusted_cnae.withColumn("atualizado_em", lit(atualizado_em))
    
    print("Tratamento concluído")

    print(f"DataFrame carregado. Esquema: {trusted_cnae.printSchema()}")
    print(f"Primeiras linhas do DataFrame: {trusted_cnae.show(5)}") 
    row_count = trusted_cnae.count()
    print(f"Número de linhas no DataFrame: {row_count}")

    # tirando configs da raw
    hadoop_conf.unset("fs.s3a.access.key")
    hadoop_conf.unset("fs.s3a.secret.key")
    hadoop_conf.unset("fs.s3a.endpoint")


    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_TRUSTED_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_TRUSTED_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_TRUSTED_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")

    print(os.getenv('MINIO_TRUSTED_ACCESS_KEY'))
    print(os.getenv('MINIO_TRUSTED_SECRET_KEY'))
    print(os.getenv('MINIO_TRUSTED_ENDPOINT'))


    print("Iniciando salvamento dos arquivos")
    trusted_cnae.write \
        .partitionBy("data_ref") \
        .format("delta") \
        .option("mergeSchema", "true") \
        .option("encoding", 'latin1') \
        .mode("overwrite") \
        .save("s3a://bureaus/receita-federal/cnaes")

    print("Arquivos Salvos")

    spark.stop()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("CNAEsToTrusted") \
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
