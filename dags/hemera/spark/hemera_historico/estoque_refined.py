# Importando bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, concat, lpad, substring, coalesce, col, to_date, when, trim, regexp_extract, input_file_name, year, month, max as spark_max, first, last, min as spark_min
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from minio import Minio
from io import BytesIO
from datetime import datetime, timezone, timedelta
import re
import os
import pandas as pd
from pyspark.sql.types import DateType


def estoque_consolidado_refined (access_params=None, **kwargs):

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


    # Lendo dados
    df_estoque = spark.read.format("delta") \
        .load("s3a://hemera-trusted/estoque")
    

    df_estoque = df_estoque.withColumn(
        "numero_titulo",
        lpad(col("numero_titulo").cast("string"), 10, "0")
    )


    df_estoque = df_estoque.withColumn("nome_cedente", trim(col("nome_cedente"))) \
        .withColumn("nome_sacado", trim(col("nome_sacado")))

    df_estoque.show(5)


    df_estoque = df_estoque.withColumn("pddtotal", col("pdd_nota") + col("pdd_vencido"))


    # Lista de datas de referência
    datas_referencia = [row['data_referencia'] for row in df_estoque.select('data_referencia').distinct().collect()]
    datas_referencia.sort()

    df_resultado_final = None

    for data_ref in datas_referencia:
        ano_ref, mes_ref = map(int, data_ref.split("-"))

        print(f"Processando data de referência: {data_ref}")
        
        # Calcular mês anterior
        if mes_ref == 1:
            ano_anterior = ano_ref - 1
            mes_anterior = 12
        else:
            ano_anterior = ano_ref
            mes_anterior = mes_ref - 1

        # 1. Filtrar registros do mês da data de referência
        df_mes_ref = df_estoque.filter(col("data_referencia") == data_ref)


        # 2. Filtrar registros do mês anterior com base na data_arquivo
        df_mes_anterior = df_estoque.filter(
            (year("data_arquivo") == ano_anterior) & (month("data_arquivo") == mes_anterior)
        )

        # Obter a última data do mês anterior
        ultima_data_anterior = df_mes_anterior.agg(spark_max("data_arquivo")).collect()[0][0]

        # Filtrar apenas os registros dessa última data
        df_ultimo_dia_mes_anterior = df_mes_anterior.filter(col("data_arquivo") == ultima_data_anterior)

        # Concatenar com os dados da data_ref
        df_completo = df_mes_ref.unionByName(df_ultimo_dia_mes_anterior)

        # Forçar a data_ref correta após a junção
        df_completo = df_completo.withColumn("data_referencia", lit(data_ref))

        # Valores base foto
        df_agrupado = df_completo.groupBy(
        "data_referencia", "id_titulo", "cnpj_sacado", "nome_sacado", "cnpj_cedente", "nome_cedente", "grupo"
        ).agg(
            spark_min("data_aquisicao").alias("data_aquisicao"),
            spark_min("data_vencimento").alias("data_vencimento"),
            spark_min("valor_nominal").alias("valor_nominal"),
            spark_min("valor_aquisicao").alias("valor_aquisicao"),
            spark_min("numero_titulo").alias("numero_titulo")
        )

        # Criar janela
        janela = Window.partitionBy("data_referencia", "id_titulo").orderBy("data_arquivo")

        # Aplicar agregações
        df_agregado_temp = df_completo \
            .withColumn("estoque_valor_presente_inicial", first("valor_presente").over(janela)) \
            .withColumn("estoque_valor_presente_final", last("valor_presente").over(janela)) \
            .withColumn("pdd_inicial", first("pddtotal").over(janela)) \
            .withColumn("pdd_final", last("pddtotal").over(janela)) \
            .withColumn("primeira_data", first("data_arquivo").over(janela)) \
            .withColumn("ultima_data", last("data_arquivo").over(janela)) \
            .select("data_referencia", "id_titulo", "estoque_valor_presente_inicial", 
                    "estoque_valor_presente_final", "primeira_data", "ultima_data", 
                    "pdd_inicial", "pdd_final") \
            .distinct()
        
        # Agora fazer um groupBy para reduzir as duplicações
        df_agregado = df_agregado_temp.groupBy("data_referencia", "id_titulo").agg(
            first("estoque_valor_presente_inicial").alias("estoque_valor_presente_inicial"),
            last("estoque_valor_presente_final").alias("estoque_valor_presente_final"),
            min("primeira_data").alias("primeira_data"),
            max("ultima_data").alias("ultima_data"),
            first("pdd_inicial").alias("pdd_inicial"),
            last("pdd_final").alias("pdd_final")
        )


        df_final = df_agrupado.join(
            df_agregado,
            on=["id_titulo", "data_referencia"],
            how="left"
        )


        df_final = df_final.dropDuplicates(["id_titulo", "data_referencia"])

        # Capturar datas de abertura e fechamento
        datas = df_completo.agg(
            spark_max("data_arquivo").alias("data_fechamento"),
            spark_min("data_arquivo").alias("data_abertura")
        ).collect()[0]

        data_fechamento = datas["data_fechamento"]
        data_abertura = datas["data_abertura"]

        print(data_fechamento)
        print(data_abertura)


        # Aplicar lógica de manter valor apenas se a data for igual à de abertura/fechamento
        df_final = df_final \
            .withColumn(
                "estoque_valor_presente_inicial",
                when(col("primeira_data") == lit(data_abertura), col("estoque_valor_presente_inicial")).otherwise(lit(0))
            ) \
            .withColumn(
                "estoque_valor_presente_final",
                when(col("ultima_data") == lit(data_fechamento), col("estoque_valor_presente_final")).otherwise(lit(0))
            ) \
            .withColumn(
                "pdd_inicial",
                when(col("primeira_data") == lit(data_abertura), col("pdd_inicial")).otherwise(lit(0))
            ) \
            .withColumn(
                "pdd_final",
                when(col("ultima_data") == lit(data_fechamento), col("pdd_final")).otherwise(lit(0))
            ) \
            .drop("primeira_data") \
            .withColumnRenamed("ultima_data", "data_fechamento") \
            .withColumn("data_fechamento", lit(data_fechamento).cast(DateType()))


        # Acumular os resultados
        if df_resultado_final is None:
            df_resultado_final = df_final
        else:
            df_resultado_final = df_resultado_final.unionByName(df_final)

    
    # Adiciona zeros à esquerda até 10 caracteres
    df_resultado_final = df_resultado_final.withColumn(
        "id_titulo", lpad(col("id_titulo").cast("string"), 10, "0")
    )


    print("Tratamento Concluído")

    # Tirando configs da raw
    hadoop_conf.unset("fs.s3a.access.key")
    hadoop_conf.unset("fs.s3a.secret.key")
    hadoop_conf.unset("fs.s3a.endpoint")

    # Colocando configs trusted
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_REFINED_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_REFINED_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_REFINED_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")

    print("Iniciando salvamento dos arquivos")
    df_resultado_final.write \
        .format("delta") \
        .option("mergeSchema", "true") \
        .option("encoding", 'latin1') \
        .mode("overwrite") \
        .save("s3a://hemera-refined/estoque/delta")

    print("Arquivos Salvos")

    spark.stop()

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("estoque_diario_hemera") \
        .config("spark.sql.encoding", "latin1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "3g") \
        .config("spark.executor.memory", "5g") \
        .config("spark.executor.cores", "4") \
        .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    estoque_consolidado_refined(spark)
