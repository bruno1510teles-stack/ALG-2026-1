# Importando bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, concat, lpad, substring, coalesce, col, to_date, when, trim, regexp_extract, input_file_name, greatest, ceil
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StringType, FloatType
from pyspark.sql import Window
from minio import Minio
from io import BytesIO
from datetime import datetime, timezone, timedelta
import re
import os
import pandas as pd
import threading
import time
import hashlib
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import numpy as np


def calcular_faturamento_estimado (spark, **kwargs):

    spark.sparkContext.setLogLevel("ERROR")

    # Configurações do Hadoop para acesso ao MinIO (S3 compatível)
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_REFINED_ACCESS_KEY')) 
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_REFINED_SECRET_KEY')) 
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_REFINED_ENDPOINT')) 
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.attempts.maximum", "1")
    hadoop_conf.set("fs.s3a.connection.establish.timeout", "10000")
    hadoop_conf.set("fs.s3a.connection.timeout", "20000")
    hadoop_conf.set("hadoop.security.authentication", "simple")
    hadoop_conf.set("hadoop.security.authorization", "false")

    print('-------------------------------------------------------------------------------------')
    print('Carregando delta dados cadastrais...')

    # Lendo dados
    df_dados_cadastrais = spark.read.format("delta") \
        .load("s3a://receita-federal/dados-cadastrais")


    df_dados_cadastrais.createOrReplaceTempView("dados_cadastrais")

    print("-------------------------------------------------------------------------------------")

    print('Iniciando tratamento de dados do delta...')

    df_dados_cadastrais = spark.sql("""
                                    select distinct
                                        cnpj_raiz
                                        , razao_social
                                        , situacao_cadastral 
                                        , municipio 
                                        , uf 
                                        , bairro 
                                        , cep 
                                        , cnae_principal_codigo as cod_cnae_principal 
                                        , cnae_principal_descricao as descricao_cnae_principal
                                        , case  -- MICRO EMPRESA
                                                when porte_empresa = 'MICRO EMPRESA' and capital_social_empresa <= 100000 then 30000
                                                when porte_empresa = 'MICRO EMPRESA' and capital_social_empresa > 100000 and capital_social_empresa <= 350000  then 40000
                                                when porte_empresa = 'MICRO EMPRESA' and capital_social_empresa > 350000 then 50000
                                                    
                                                -- EMPRESA DE PEQUENO PORTE
                                                when porte_empresa = 'EMPRESA DE PEQUENO PORTE' and capital_social_empresa <= 100000 then 40000
                                                when porte_empresa = 'EMPRESA DE PEQUENO PORTE' and capital_social_empresa > 100000 and capital_social_empresa <= 350000  then 50000
                                                when porte_empresa = 'EMPRESA DE PEQUENO PORTE' and capital_social_empresa > 350000 then 70000
                                                    
                                                -- MÉDIA
                                                when porte_empresa = 'DEMAIS' and capital_social_empresa <= 100000 then 100000
                                                when porte_empresa = 'DEMAIS' and capital_social_empresa > 100000 and capital_social_empresa <= 350000  then 100000
                                                when porte_empresa = 'DEMAIS' and capital_social_empresa > 350000 then 100000
                                                    
                                                else 30000 end as faturamento_receita_federal
                                    from dados_cadastrais
                                    where flag_matriz = 'Sim'
                                    and capital_social_empresa > 0
                                    """
    )   

    print("-------------------------------------------------------------------------------------")

    print('Carregando dados do Trino - Fat HP Externo e Serasa')

    # Conexão com o Trino
    conn = connect(
        host=os.getenv("TRINO_ENDPOINT"),
        port=os.getenv("TRINO_PORT"),
        user=os.getenv("TRINO_USER"),
        auth=BasicAuthentication(os.getenv("TRINO_USER"), os.getenv("TRINO_PASSWORD")),
        http_scheme="https",
    )

    def execute_query(conn, query):
        cur = conn.cursor()  # Abre o cursor
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()  # Fecha o cursor após a execução
        
        return pd.DataFrame(rows, columns=columns)


    # Base Faturamento Externo Arcelor
    query_fat_externo = """
                select
                    raiz_cnpj as cnpj_raiz,
                    cast(max(media_vop_total) as double) as faturamento_externo_arcelor
                from deltalakerefined.payments.faturamento_externo_arcelor
                group by raiz_cnpj
                """
    faturamento_externo_hp = execute_query(conn, query_fat_externo)



    # Base Faturamento Serasa
    query_fat_serasa = f"""
                        select
                            substring(ir.document_number,1,8) as cnpj_raiz,
                            round(coalesce((pontual.historical_average_range_from + pontual.historical_average_range_to) / 2.0, 0), 2) AS faturamento_serasa_pagamento
                        from postgres.exrp_{os.getenv('STAGE')}_default.report r
                                inner join postgres.exrp_{os.getenv('STAGE')}_default.identification_report ir on ir.id = r.identification_report_id
                                inner join (select document_number, max(id) as max_id from postgres.exrp_{os.getenv('STAGE')}_default.identification_report group by document_number) ir2 on ir2.max_id = r.identification_report_id
                                inner join postgres.exrp_{os.getenv('STAGE')}_default.advanced_commercial_payment_history acph on acph.id = r.advanced_commercial_payment_history_id
                                inner join postgres.exrp_{os.getenv('STAGE')}_default.payment_history ph on ph.id = acph.payment_history_id
                                inner join postgres.exrp_{os.getenv('STAGE')}_default.month_detail md on md.id = ph.month_detail_id
                                inner join postgres.exrp_{os.getenv('STAGE')}_default.total_summary ts on ts.id = md.summary_id
                                inner join postgres.exrp_{os.getenv('STAGE')}_default.period pontual on pontual.id = ts.punctual_id
                                inner join postgres.exrp_{os.getenv('STAGE')}_default.period total on total.id = ts.total_id
                        """
    faturamento_serasa = execute_query(conn, query_fat_serasa)


    ### Transformando DFs em Spark

    fat_externo_hp = spark.createDataFrame(faturamento_externo_hp)

    fat_serasa = spark.createDataFrame(faturamento_serasa)


    print("-------------------------------------------------------------------------------------")

    print('Unificando e tratando os dados...')

    df_final = df_dados_cadastrais \
        .join(fat_externo_hp, on="cnpj_raiz", how="left") \
        .join(fat_serasa, on="cnpj_raiz", how="left")


    ### Tratando Nulos
    df_final = df_final.withColumn(
        "faturamento_receita_federal",
        when(col("faturamento_receita_federal").isNull(), 0).otherwise(col("faturamento_receita_federal"))
    )

    df_final = df_final.withColumn(
        "faturamento_externo_arcelor",
        when(col("faturamento_externo_arcelor").isNull(), 0).otherwise(col("faturamento_externo_arcelor"))
    )

    df_final = df_final.withColumn(
        "faturamento_serasa_pagamento",
        when(col("faturamento_serasa_pagamento").isNull(), 0).otherwise(col("faturamento_serasa_pagamento"))
    )


    ### Transformando em Float
    df_final = df_final \
        .withColumn("faturamento_receita_federal", col("faturamento_receita_federal").cast("float")) \
        .withColumn("faturamento_externo_arcelor", col("faturamento_externo_arcelor").cast("float")) \
        .withColumn("faturamento_serasa_pagamento", col("faturamento_serasa_pagamento").cast("float"))
    

    print("-------------------------------------------------------------------------------------")

    print('Calculando Faturamento Estimado...')


    ### Criando colunas com as equações das regressões

    df_final = df_final \
        .withColumn("apenas_receita", 1.4843 * col("faturamento_receita_federal")) \
        .withColumn("apenas_serasa", 0.2976 * col("faturamento_serasa_pagamento")) \
        .withColumn("apenas_hp_externo", 0.8031 * col("faturamento_externo_arcelor")) \
        .withColumn("hp_externo_e_receita", 
            0.39584 * col("faturamento_externo_arcelor") +
            1.29748 * col("faturamento_receita_federal")
        ) \
        .withColumn("receita_e_serasa", 
            1.24851 * col("faturamento_receita_federal") +
            0.13639 * col("faturamento_serasa_pagamento")
        ) \
        .withColumn("receita_hp_externo_e_serasa", 
            0.25467 * col("faturamento_externo_arcelor") +
            1.25549 * col("faturamento_receita_federal") +
            0.07835 * col("faturamento_serasa_pagamento")
        )
    

    # Calcula o valor máximo entre as colunas estimadas
    df_final = df_final.withColumn(
        "faturamento_estimado",
        greatest(
            col("apenas_receita"),
            col("apenas_serasa"),
            col("apenas_hp_externo"),
            col("hp_externo_e_receita"),
            col("receita_e_serasa"),
            col("receita_hp_externo_e_serasa")
        )
    )

    # Origem do calculo
    df_final = df_final.withColumn(
        "fonte_faturamento",
        when(col("faturamento_estimado") == col("hp_externo_e_receita"), "MODELO B1")
        .when(col("faturamento_estimado") == col("receita_e_serasa"), "MODELO B2")
        .when(col("faturamento_estimado") == col("receita_hp_externo_e_serasa"), "MODELO C1")
        .when(col("faturamento_estimado") == col("apenas_serasa"), "MODELO A2")
        .when(col("faturamento_estimado") == col("apenas_hp_externo"), "MODELO A3")
        .when(col("faturamento_estimado") == col("apenas_receita"), "MODELO A1")
        .otherwise("NÃO ENCONTRADO")
    )


    # De/Para dos modelos utilizados:
    # Modelo A1 = Apenas Receita Federal
    # Modelo A2 = Apenas Serasa
    # Modelo A3 = Apenas HP Externo
    # Modelo B1 = Receita Federal + HP Externo
    # Modelo B2 = Receita Federal + Serasa
    # Modelo C1 = Receita Federal + Serasa + HP Externo


    # Arredondamento 5K
    df_final = df_final.withColumn(
        "faturamento_estimado",
        ceil(col("faturamento_estimado") / 5000) * 5000
    )

    
    print("-------------------------------------------------------------------------------------")

    print('Organizando as colunas para exportação...')


    df_final = df_final.select(
        "cnpj_raiz",
        "razao_social",
        "situacao_cadastral",
        "municipio",
        "uf",
        "bairro",
        "cep",
        "cod_cnae_principal",
        "descricao_cnae_principal",
        "fonte_faturamento",
        "faturamento_estimado"
    )

    df_final = df_final.select(
        col("cnpj_raiz").cast(StringType()),
        col("razao_social").cast(StringType()),
        col("situacao_cadastral").cast(StringType()),
        col("municipio").cast(StringType()),
        col("uf").cast(StringType()),
        col("bairro").cast(StringType()),
        col("cep").cast(StringType()),
        col("cod_cnae_principal").cast(StringType()),
        col("descricao_cnae_principal").cast(StringType()),
        col("fonte_faturamento").cast(StringType()),
        col("faturamento_estimado").cast(FloatType())
    )


    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    atualizado_em = now.strftime('%Y-%m-%d %X')  
    df_final = df_final.withColumn("atualizado_em", lit(atualizado_em))


    print("Tratamentos Finalizados")


    print("-------------------------------------------------------------------------------------")


    print('Salvando arquivo...')

    def keep_alive_logger(interval=60):
        """Thread para imprimir mensagens de keep-alive."""
        while not done_flag.is_set():
            print("[INFO] Processando escrita no Delta... ainda rodando.")
            time.sleep(interval)

    done_flag = threading.Event()
    logger_thread = threading.Thread(target=keep_alive_logger)
    logger_thread.start()


    # Tirando configs
    hadoop_conf.unset("fs.s3a.access.key")
    hadoop_conf.unset("fs.s3a.secret.key")
    hadoop_conf.unset("fs.s3a.endpoint")

    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_REFINED_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_REFINED_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_REFINED_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")


    try:
        df_final.write \
            .format("delta") \
            .option("overwriteSchema", "true") \
            .mode("overwrite") \
            .save("s3a://motor/faturamento_estimado")

        print("[INFO] Arquivos Salvos com sucesso.")  
    finally:
        done_flag.set()
        logger_thread.join()


    spark.stop()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("CalculaFaturamentoEstimado") \
        .config("spark.sql.encoding", "latin1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "100") \
        .config("spark.shuffle.compress", "true") \
        .config("spark.shuffle.spill.compress", "true") \
        .config("spark.shuffle.file.buffer", "64k") \
        .config("spark.local.dir", "/tmp/spark") \
        .config("spark.driver.extraJavaOptions", "-Divy.cache.dir=/tmp -Divy.home=/tmp") \
        .config("spark.executor.extraJavaOptions", "-Divy.cache.dir=/tmp -Divy.home=/tmp") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    calcular_faturamento_estimado(spark)