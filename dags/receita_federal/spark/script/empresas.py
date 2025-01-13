from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from minio import Minio
import os
from itertools import chain
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
import argparse


def empresas_to_trusted(spark):
    # Função para gerar cliente MinIO
    def generate_client_minio():
        parsedurl = os.getenv('MINIO_RAW_ENDPOINT')
        return Minio(
            parsedurl,
            os.getenv('MINIO_RAW_ACCESS_KEY'),
            os.getenv('MINIO_RAW_SECRET_KEY')
        )

    # Função para recuperar o arquivo mais recente no bucket
    def get_newest_file_path(client, bucket, path): 
        objects = client.list_objects(bucket, f'{path}/')
        max_ano = max(obj.object_name for obj in objects)
        
        objects = client.list_objects(bucket, max_ano)
        max_particao = max(obj.object_name for obj in objects)
        
        return max_particao

    # Configuração das credenciais do MinIO
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

    # Criando o cliente MinIO
    client = generate_client_minio()

    # Pastas MiniO
    BUCKET_SOURCE = "receita-federal"
    PATH = "empresas"
    
    print("Lendo Pasta mais recente...")
    file_path = get_newest_file_path(client, BUCKET_SOURCE, PATH)
    print("Pasta mais recente: {}".format(file_path))

    objects = client.list_objects("receita-federal", prefix=file_path)

    # Extract file names
    file_names = [obj.object_name for obj in objects]

    print("Arquivos a serem processados:")
    for file_name in file_names:
        print(file_name)
    
    # Inicializando o DataFrame vazio para armazenar todos os dados
    trusted_empresas = None

    # Lendo todos os arquivos encontrados no file_path
    for file_name in file_names:
        print(f"Lendo arquivo: {file_name}")
        df_temp = spark.read \
            .option("delimiter", ";") \
            .option("header", False) \
            .option("encoding", 'latin1') \
            .csv(f"s3a://{BUCKET_SOURCE}/{file_name}")
        
        # Concatenando os DataFrames
        if trusted_empresas is None:
            trusted_empresas = df_temp
        else:
            trusted_empresas = trusted_empresas.union(df_temp)

    print("Todos os arquivos foram lidos e combinados.")



    header = ['cnpj_raiz',
        'razao_social',
        'natureza_juridica',
        'qualificacao_responsavel',
        'capital_social_empresa',
        'codigo_porte_empresa',
        'ente_federativo_responsavel']
    
    trusted_empresas_tratado  = trusted_empresas \
        .withColumnRenamed("_c0", header[0]) \
        .withColumnRenamed("_c1", header[1]) \
        .withColumnRenamed("_c2", header[2]) \
        .withColumnRenamed("_c3", header[3]) \
        .withColumnRenamed("_c4", header[4]) \
        .withColumnRenamed("_c5", header[5]) \
        .withColumnRenamed("_c6", header[6]) 
        
    print("Colunas Renomeadas")    
    trusted_empresas_tratado  = trusted_empresas_tratado.withColumn(
        'capital_social_empresa',
        F.regexp_replace(F.col('capital_social_empresa'), ',', '.').cast('float')
    )
    
    # Dicionário de mapeamento
    porte_empresa_dict = {
        '00': 'NÃO INFORMADO',
        '01': 'MICRO EMPRESA',
        '03': 'EMPRESA DE PEQUENO PORTE',
        '05': 'DEMAIS'
    }

    # Cria um mapa (coluna de mapeamento)
    mapping_expr = F.create_map([F.lit(x) for x in chain(*porte_empresa_dict.items())])

    # Aplica o mapeamento e usa 'UNKNOWN' como valor padrão
    trusted_empresas_tratado  = trusted_empresas_tratado.withColumn(
        "porte_empresa",
        F.coalesce(mapping_expr[F.col("codigo_porte_empresa")], F.lit("UNKNOWN"))
    )
    
    colunas_ordem = [
        'cnpj_raiz',
        'razao_social',
        'natureza_juridica',
        'qualificacao_responsavel',
        'capital_social_empresa',
        'codigo_porte_empresa',
        'porte_empresa',
        'ente_federativo_responsavel'
    ]

    # Reordenar as colunas usando select
    trusted_empresas_tratado = trusted_empresas_tratado.select(*colunas_ordem)
    
    
    # Extraindo o ano e o mês do caminho do arquivo
    ano = file_path.split("year=")[1].split("/")[0]
    mes = file_path.split("month=")[1].split("/")[0]
    # Construindo o ANOMES no formato desejado
    data_ref = ano + mes
    # Adicionando a coluna "data_ref" ao DataFrame
    trusted_empresas_tratado = trusted_empresas_tratado.withColumn("data_ref", lit(data_ref))
    
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    atualizado_em = now.strftime('%Y-%m-%d %X')  
    trusted_empresas_tratado = trusted_empresas_tratado.withColumn("atualizado_em", lit(atualizado_em))

    print(f"DataFrame carregado. Esquema: {trusted_empresas_tratado.printSchema()}")
    print(f"Primeiras linhas do DataFrame: {trusted_empresas_tratado.show(5)}") 
    row_count = trusted_empresas_tratado.count()
    print(f"Número de linhas no DataFrame: {row_count}")

    trusted_empresas_tratado.unpersist()
    spark.catalog.clearCache()

    # Reconfigurar a sessão Spark para escrita no segundo MinIO
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_TRUSTED_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_TRUSTED_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_TRUSTED_ENDPOINT'))
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")


    print("Iniciando salvamento dos arquivos")
    # Escrevendo os dados com o schema definido
    trusted_empresas_tratado.write \
        .partitionBy("data_ref") \
        .format("delta") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .save("s3a://bureaus/receita-federal/empresas")


    print("Arquivos Salvos")    
    # Fechar a sessão Spark
    spark.stop()

        
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("EmpresasToTrusted") \
        .config("spark.sql.encoding", "latin1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    
    empresas_to_trusted(spark)