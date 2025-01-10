from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql import functions as F
from minio import Minio
import os
from datetime import datetime, timezone, timedelta
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logging.debug("Mensagem de depuração")
logging.info("Processo iniciado com sucesso")
logging.warning("Aviso: Algo pode estar errado")
logging.error("Erro encontrado")
logging.critical("Erro crítico! Sistema parado")

def socios_to_trusted(spark):
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
    PATH = "socios"
    
    print("Lendo Pasta mais recente...")
    file_path = get_newest_file_path(client, BUCKET_SOURCE, PATH)
    print("Pasta mais recente:: {}".format(file_path))

    objects = client.list_objects("receita-federal", prefix=file_path)

    # Extract file names
    file_names = [obj.object_name for obj in objects]

    print("Arquivos a serem processados:")
    for file_name in file_names:
        print(file_name)
    

    # Inicializando o DataFrame vazio para armazenar todos os dados
    trusted_socios = None

    # Lendo todos os arquivos encontrados no file_path
    for file_name in file_names:
        print(f"Lendo arquivo: {file_name}")
        df_temp = spark.read \
            .option("delimiter", ";") \
            .option("header", False) \
            .option("encoding", 'latin1') \
            .csv(f"s3a://{BUCKET_SOURCE}/{file_name}")
        
        # Concatenando os DataFrames
        if trusted_socios is None:
            trusted_socios = df_temp
        else:
            trusted_socios = trusted_socios.union(df_temp)

    # Renomeando colunas        
    header_inicial = ['CNPJ BÁSICO',
                    'IDENTIFICADOR DE SÓCIO',
                    'NOME DO SÓCIO OU RAZÃO SOCIAL',
                    'CNPJ/CPF DO SÓCIO',
                    'QUALIFICAÇÃO DO SÓCIO',
                    'DATA DE ENTRADA SOCIEDADE',
                    'PAIS',
                    'REPRESENTANTE LEGAL',
                    'NOME DO REPRESENTANTE',
                    'QUALIFICAÇÃO DO REPRESENTANTE LEGAL',
                    'FAIXA ETÁRIA']
    
    trusted_socios = trusted_socios \
        .withColumnRenamed("_c0", header_inicial[0]) \
        .withColumnRenamed("_c1", header_inicial[1]) \
        .withColumnRenamed("_c2", header_inicial[2]) \
        .withColumnRenamed("_c3", header_inicial[3]) \
        .withColumnRenamed("_c4", header_inicial[4]) \
        .withColumnRenamed("_c5", header_inicial[5]) \
        .withColumnRenamed("_c6", header_inicial[6]) \
        .withColumnRenamed("_c7", header_inicial[7]) \
        .withColumnRenamed("_c8", header_inicial[8]) \
        .withColumnRenamed("_c9", header_inicial[9]) \
        .withColumnRenamed("_c10", header_inicial[10]) 
    
    # Dicionario de-para faixa etária  
    faixa_etaria_dict = {'1': '0 a 12 anos',
                     '2': '13 a 20 anos',
                     '3': '21 a 30 anos',
                     '4': '31 a 40 anos',
                     '5': '41 a 50 anos',
                     '6': '51 a 60 anos',
                     '7': '61 a 70 anos',
                     '8': '71 a 80 anos',
                     '9': 'mais de 80 anos',
                     '0': None}

    # Dicionario de-para identificador de pessoa
    identificador_socio_dict = {
        '1': 'PESSOA JURÍDICA',
        '2': 'PESSOA FÍSICA',
        '3': 'ESTRANGEIRO'
    }

    # Função UDF para aplicar os mapeamentos e converter os códigos de identificador e de faixa etária nos seus respectivos valores
    faixa_etaria_udf = F.udf(lambda x: faixa_etaria_dict.get(x, None), StringType())
    identificador_socio_udf = F.udf(lambda x: identificador_socio_dict.get(x, None), StringType())

    # Aplicando os dicionários de mapeamento
    trusted_socios = trusted_socios.withColumn('FAIXA ETÁRIA', faixa_etaria_udf(F.col('FAIXA ETÁRIA')))
    trusted_socios = trusted_socios.withColumn('IDENTIFICADOR DE SÓCIO', identificador_socio_udf(F.col('IDENTIFICADOR DE SÓCIO')))

    # Tratando a coluna de data
    trusted_socios = trusted_socios.withColumn(
        'DATA DE ENTRADA SOCIEDADE',
        F.concat_ws('-', 
                    F.col('DATA DE ENTRADA SOCIEDADE').substr(1, 4), 
                    F.col('DATA DE ENTRADA SOCIEDADE').substr(5, 2), 
                    F.col('DATA DE ENTRADA SOCIEDADE').substr(7, 2))
    )

    # Renomeando para nome final das colunas
    header_final = {'CNPJ BÁSICO': 'cnpj_raiz',
        'IDENTIFICADOR DE SÓCIO': 'identificador_socio',
        'NOME DO SÓCIO OU RAZÃO SOCIAL': 'nome/razao_social',
        'CNPJ/CPF DO SÓCIO': 'documento_socio',
        'QUALIFICAÇÃO DO SÓCIO': 'qualificacao_socio',
        'DATA DE ENTRADA SOCIEDADE': 'data_entrada_sociedade',
        'PAIS': 'pais',
        'REPRESENTANTE LEGAL': 'representante_legal',
        'NOME DO REPRESENTANTE': 'nome_representante',
        'QUALIFICAÇÃO DO REPRESENTANTE LEGAL': 'qualificacao_representante',
        'FAIXA ETÁRIA': 'faixa_etaria_socio'}
    
    trusted_socios = trusted_socios \
        .withColumnRenamed('CNPJ BÁSICO', header_final['CNPJ BÁSICO']) \
        .withColumnRenamed('IDENTIFICADOR DE SÓCIO', header_final['IDENTIFICADOR DE SÓCIO']) \
        .withColumnRenamed('NOME DO SÓCIO OU RAZÃO SOCIAL', header_final['NOME DO SÓCIO OU RAZÃO SOCIAL']) \
        .withColumnRenamed('CNPJ/CPF DO SÓCIO', header_final['CNPJ/CPF DO SÓCIO']) \
        .withColumnRenamed('QUALIFICAÇÃO DO SÓCIO', header_final['QUALIFICAÇÃO DO SÓCIO']) \
        .withColumnRenamed('DATA DE ENTRADA SOCIEDADE', header_final['DATA DE ENTRADA SOCIEDADE']) \
        .withColumnRenamed('PAIS', header_final['PAIS']) \
        .withColumnRenamed('REPRESENTANTE LEGAL', header_final['REPRESENTANTE LEGAL']) \
        .withColumnRenamed('NOME DO REPRESENTANTE', header_final['NOME DO REPRESENTANTE']) \
        .withColumnRenamed('QUALIFICAÇÃO DO REPRESENTANTE LEGAL', header_final['QUALIFICAÇÃO DO REPRESENTANTE LEGAL']) \
        .withColumnRenamed('FAIXA ETÁRIA', header_final['FAIXA ETÁRIA']) 


    # Extraindo o ano e o mês do caminho do arquivo
    ano = file_path.split("year=")[1].split("/")[0]
    mes = file_path.split("month=")[1].split("/")[0]
    # Construindo o ANOMES no formato desejado
    data_ref = ano + mes
    # Adicionando a coluna "data_ref" ao DataFrame
    trusted_socios = trusted_socios.withColumn("data_ref", lit(data_ref))

    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    atualizado_em = now.strftime('%Y-%m-%d %X')  
    trusted_socios = trusted_socios.withColumn("atualizado_em", lit(atualizado_em))

    print(f"DataFrame carregado. Esquema: {trusted_socios.printSchema()}")
    print(f"Primeiras linhas do DataFrame: {trusted_socios.show(5)}") 
    row_count = trusted_socios.count()
    print(f"Número de linhas no DataFrame: {row_count}")   

    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_TRUSTED_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_TRUSTED_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_TRUSTED_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")

    print("Iniciando salvamento dos arquivos")
    trusted_socios.write \
        .partitionBy("data_ref") \
        .format("delta") \
        .option("mergeSchema", "true") \
        .option("encoding", 'latin1') \
        .mode("overwrite") \
        .save("s3a://teste-felipe/receita-federal/socios")

    print("Arquivos Salvos")

    spark.stop()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("sociosToTrusted") \
        .config("spark.sql.encoding", "latin1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    
    socios_to_trusted(spark)
