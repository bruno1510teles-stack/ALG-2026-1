from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import lit, concat, lpad, substring, coalesce, col, to_date, when, format_number, regexp_replace
from pyspark.sql.types import DoubleType, DateType
import os
from datetime import datetime, timezone, timedelta
import pandas as pd
from minio import Minio
from io import BytesIO

def estoque_consolidado(spark):
    # Colocando Configs
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

    # Lendo arquivos
    print("Lendo Arquivos")
    df = spark.read.format("delta").load("s3a://hemera-trusted/estoque")
    print("Arquivos Lidos")

    print("Iniciando Tratamento Dados")
    # Tratar a coluna "DataPosicao" para lidar com valores no formato Excel e já formatados
    df = df.withColumn(
        "valor_aquisicao_tratado",
        F.when(F.col("data_vencimento") > F.date_sub(F.col("data_arquivo"), 60), F.col("valor_aquisicao"))
        .otherwise(0)
    )

    df = df.withColumn(
        "valor_aquisicao_vendermais",
        F.when(F.col("produto") == "VenderMais" , F.col("valor_aquisicao_tratado"))
        .otherwise(0)
    )

    df = df.withColumn(
        "valor_aquisicao_tradicional",
        F.when(F.col("produto") == "Tradicional" , F.col("valor_aquisicao_tratado"))
        .otherwise(0)
    )

    df = df.withColumn(
        "valor_aquisicao_conglomerado",
        F.when(F.col("produto") == "Conglomerado" , F.col("valor_aquisicao_tratado"))
        .otherwise(0)
    )

    df = df.groupBy("data_arquivo", "data_referencia").agg(
        F.sum("valor_presente").alias("estoque"),
        (F.sum("pdd_nota") + F.sum("pdd_vencido")).alias("pdd"),
        F.sum("valor_aquisicao_vendermais").alias("valor_aquisicao_vendermais"),
        F.sum("valor_aquisicao_tradicional").alias("valor_aquisicao_tradicional"),
        F.sum("valor_aquisicao_conglomerado").alias("valor_aquisicao_conglomerado")
    )


    # Trazendo dados da Selic
    print("Trazendo dados Selic")
    client = Minio(
                os.getenv('MINIO_RAW_ENDPOINT'),
                access_key=os.getenv('MINIO_RAW_ACCESS_KEY'),
                secret_key=os.getenv('MINIO_RAW_SECRET_KEY'), 
                secure=True
            )


    # Definindo bucket e caminho do arquivo
    BUCKET_SOURCE_RAW = "auxiliares"
    FOLDER_DESTINATION_RAW = 'selic'
    file_name = 'AUXILIAR_SELIC.xlsx'
    file_path = f'{FOLDER_DESTINATION_RAW}/{file_name}'

    # Obtendo o arquivo do MinIO
    response = client.get_object(BUCKET_SOURCE_RAW, file_path)
    # Lendo os dados
    file_data = BytesIO(response.read())
    df_selic = pd.read_excel(file_data)
    # Fechando o response para liberar recursos
    response.close()
    response.release_conn()
    # Exibindo e confirmando a coleta dos dados
    print('Dados da Selic coletados com sucesso!')

    selic = spark.createDataFrame(df_selic)
    selic = selic.select("DATA", "SELIC_DIA")

    df = df.withColumn("data_arquivo", F.to_date(F.col("data_arquivo"), "yyyy-MM-dd"))
    selic = selic.withColumn("DATA", F.to_date(F.col("DATA"), "yyyy-MM-dd"))

    print("Cruzando DF's")
    # Realizando join
    df_final = df.join(selic, df["data_arquivo"] == selic["DATA"], "inner")

    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    atualizado_em = now.strftime('%Y-%m-%d %X')  
    df_final = df_final.withColumn("atualizado_em", lit(atualizado_em))

    df_final = df_final.select(
        F.col("data_arquivo").alias("data"),
        F.col("estoque"),
        F.col("pdd"),
        F.col("valor_aquisicao_vendermais"),
        F.col("valor_aquisicao_tradicional"),
        F.col("valor_aquisicao_conglomerado"),
        F.col("SELIC_DIA").alias("selic_dia"),
        F.col("data_referencia"),
        F.col("atualizado_em")
    )

    # Formatando colunas de data
    df_final = df_final.withColumn("data", to_date(substring(col("data"), 1, 10), "yyyy-MM-dd"))

    # Formatando colunas de valores
    df_final = df_final.withColumn("estoque", col("estoque").cast(DoubleType())) \
        .withColumn("pdd", col("pdd").cast(DoubleType())) \
        .withColumn("valor_aquisicao_vendermais", col("valor_aquisicao_vendermais").cast(DoubleType())) \
        .withColumn("valor_aquisicao_tradicional", col("valor_aquisicao_tradicional").cast(DoubleType())) \
        .withColumn("valor_aquisicao_conglomerado", col("valor_aquisicao_conglomerado").cast(DoubleType())) \
        .withColumn("selic_dia", col("selic_dia").cast(DoubleType()))

    # Exibição
    df_final_exibicao = df_final.select(
        F.col("data").alias("data"),
        format_number("estoque", 2).alias("estoque_formatado"),
        format_number("pdd", 2).alias("pdd_formatado"),
        format_number("valor_aquisicao_vendermais", 2).alias("valor_aquisicao_vendermais_formatado"),
        format_number("valor_aquisicao_tradicional", 2).alias("valor_aquisicao_tradicional_formatado"),
        format_number("valor_aquisicao_conglomerado", 2).alias("valor_aquisicao_conglomerado"),
        format_number("selic_dia", 10).alias("selic_dia_formatado")
    )
    df_final_exibicao.show()
    
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
    df_final.write \
        .partitionBy("data_referencia", "data") \
        .format("delta") \
        .option("mergeSchema", "true") \
        .option("encoding", 'latin1') \
        .mode("overwrite") \
        .save("s3a://hemera/estoque_consolidado")

    print("Arquivos Salvos")


    spark.stop()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("estoque_consolidado") \
        .config("spark.sql.encoding", "latin1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    estoque_consolidado(spark)