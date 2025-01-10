from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql import functions as F
from minio import Minio
import os
from datetime import datetime, timezone, timedelta

def simples_to_trusted(spark):
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
    PATH = "simples"
    
    print("Lendo Pasta mais recente...")
    file_path = get_newest_file_path(client, BUCKET_SOURCE, PATH)
    print("Pasta mais recente:: {}".format(file_path))

    trusted_simples = spark.read \
        .option("delimiter", ";") \
        .option("header", False)\
        .option("encoding", 'latin1')\
        .csv(f"s3a://{BUCKET_SOURCE}/{file_path}")
    print("Readed data: {}".format(file_path))

    trusted_simples = trusted_simples \
        .withColumnRenamed("_c0", 'CNPJ BÁSICO') \
        .withColumnRenamed("_c1", 'OPÇÃO PELO SIMPLES') \
        .withColumnRenamed("_c2", 'DATA DE OPÇÃO PELO SIMPLES') \
        .withColumnRenamed("_c3", 'DATA DE EXCLUSÃO DO SIMPLES') \
        .withColumnRenamed("_c4", 'OPÇÃO PELO MEI') \
        .withColumnRenamed("_c5", 'DATA DE OPÇÃO PELO MEI') \
        .withColumnRenamed("_c6", 'DATA DE EXCLUSÃO DO MEI') 

    # Converter colunas de 'S' para True e qualquer outra coisa para False
    trusted_simples = trusted_simples.withColumn('OPÇÃO PELO SIMPLES', F.col('OPÇÃO PELO SIMPLES') == 'S')
    trusted_simples = trusted_simples.withColumn('OPÇÃO PELO MEI', F.col('OPÇÃO PELO MEI') == 'S')

    # Renomear colunas
    renomeando = {
        'CNPJ BÁSICO': 'cnpj_raiz',
        'OPÇÃO PELO SIMPLES': 'is_simples',
        'DATA DE OPÇÃO PELO SIMPLES': 'data_opcao_pelo_simples',
        'DATA DE EXCLUSÃO DO SIMPLES': 'data_exclusao_simples',
        'OPÇÃO PELO MEI': 'is_mei',
        'DATA DE OPÇÃO PELO MEI': 'data_opcao_pelo_mei',
        'DATA DE EXCLUSÃO DO MEI': 'data_exclusao_mei'
    }

    for old_name, new_name in renomeando.items():
        trusted_simples = trusted_simples.withColumnRenamed(old_name, new_name)

    # Substituir '00000000' por None
    cols_to_replace = ['data_opcao_pelo_simples', 'data_exclusao_simples', 'data_opcao_pelo_mei', 'data_exclusao_mei']
    for col in cols_to_replace:
        trusted_simples = trusted_simples.withColumn(col, F.when(F.col(col) == '00000000', None).otherwise(F.col(col)))

    # Formatar as datas no formato 'yyyy-MM-dd'
    for col in cols_to_replace:
        trusted_simples = trusted_simples.withColumn(col, F.when(F.col(col).isNotNull(), 
                        F.concat(F.col(col).substr(1, 4), F.lit('-'), F.col(col).substr(5, 2), F.lit('-'), F.col(col).substr(7, 2)))
                        .otherwise(None))


    # Extraindo o ano e o mês do caminho do arquivo
    ano = file_path.split("year=")[1].split("/")[0]
    mes = file_path.split("month=")[1].split("/")[0]
    # Construindo o ANOMES no formato desejado
    data_ref = ano + mes
    # Adicionando a coluna "data_ref" ao DataFrame
    trusted_simples = trusted_simples.withColumn("data_ref", lit(data_ref))

    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    atualizado_em = now.strftime('%Y-%m-%d %X')  
    trusted_simples = trusted_simples.withColumn("atualizado_em", lit(atualizado_em))

    print(f"DataFrame carregado. Esquema: {trusted_simples.printSchema()}")
    print(f"Primeiras linhas do DataFrame: {trusted_simples.show(5)}") 
    row_count = trusted_simples.count()
    print(f"Número de linhas no DataFrame: {row_count}") 

    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_TRUSTED_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_TRUSTED_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_TRUSTED_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")

    print("Iniciando salvamento dos arquivos")
    trusted_simples.write \
        .partitionBy("data_ref") \
        .format("delta") \
        .option("mergeSchema", "true") \
        .option("encoding", 'latin1') \
        .mode("overwrite") \
        .save("s3a://bureaus/receita-federal/simples")

    print("Arquivos Salvos")

    spark.stop()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("simplesToTrusted") \
        .config("spark.sql.encoding", "latin1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    
    simples_to_trusted(spark)
