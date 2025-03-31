from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import lit, coalesce, col, to_date, when, input_file_name, regexp_extract, substring, last_day, date_format, StringType, sum, max, year, month
from pyspark.sql.types import DoubleType, DateType
from datetime import datetime, timezone, timedelta
import re
import os

def hemera_raw_to_trusted_retorno(spark, **kwargs):
    # Configuração do Hadoop para acessar o S3 (MinIO)
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_TRUSTED_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_TRUSTED_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_TRUSTED_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    print("Minio Acessado")

    # Lendo arquivos
    print("Iniciando leitura dos arquivos")
    
    nome_bucket = "hemera-diario"
    caminho = "retorno_diaria"
    anos = ['2024','2025']
    meses = [str(m).zfill(2) for m in range(1, 13)]

    df_list = []

    for ano in anos:
        for mes in meses:
            caminho_csv = f"s3a://{nome_bucket}/{caminho}/year={ano}/month={mes}/"
            

            try:
                df_ano = spark.read \
                    .format("delta") \
                    .load(caminho_csv)

                # Adicionar a coluna 'data_arquivo' com o último dia do mês
                data_arquivo = datetime(int(ano), int(mes), 1).date()
                df_ano = df_ano.withColumn("data_arquivo", lit(str(data_arquivo)))

                # Adicionar a coluna 'data_ref' no formato "YYYY-MM"
                data_ref = f"{ano}-{mes.zfill(2)}"  # Garantir que o mês tenha dois dígitos
                df_ano = df_ano.withColumn("data_ref", lit(data_ref)) 

                df_list.append(df_ano)
                print(f"Arquivos lidos para {ano}/{mes}")
            
            except Exception as e:
                print(f"Erro ao ler arquivos para {ano}/{mes}: {e}")

    # Função para extrair a data do nome do arquivo
    def extract_date_from_filename(filename):
        # Expressão regular para capturar o padrão de data (dd.mm.yy)
        date_match = re.search(r'(\d{2}\.\d{2}\.\d{2})', filename)
        if date_match:
            # Converter a string de data para o formato datetime
            return datetime.strptime(date_match.group(1), '%d.%m.%y').date()
        return None

    print("Finalizado a leitura dos arquivos")

    if df_list:
        df = df_list[0]  
        for df_atual in df_list[1:]:
            df = df.unionByName(df_atual, allowMissingColumns=True)
    else:
        print("Nenhum arquivo encontrado. Encerrando a função.")
        return None

    print("Arquivos Lidos")
    df_consolidado = df
    
    # Criar coluna 'data_fechamento' como o último dia do mês
    df_consolidado = df_consolidado.withColumn("data_fechamento", last_day(col("data_arquivo")))

    # Selecionar e reorganizar as colunas
    colunas = ["data_fechamento"] + [col for col in df_consolidado.columns if col != "data_fechamento"]
    df_consolidado = df_consolidado.select(*colunas)

    # Converter colunas de datas para formato correto
    colunas_para_converter_datetime = ["data_vencimento", "data_fechamento", "data_arquivo"]
    for coluna in colunas_para_converter_datetime:
        df_consolidado = df_consolidado.withColumn(coluna, col(coluna).cast("date"))

    # Agrupamento dos dados
    df_agrupado = df_consolidado.groupBy("data_ref", "id_registro").agg(
        sum("valor_pagamento").alias("valor_pagamento"),
        max("data_arquivo").alias("data_lancamento"),
        max("data_fechamento").alias("data_fechamento")
    )

    # Adicionar timestamp de atualização
    now = datetime.now(tz=timezone(timedelta(hours=-3))).strftime('%Y-%m-%d %H:%M:%S')
    df_agrupado = df_agrupado.withColumn("atualizado_em", lit(now).cast(StringType()))

    # Adicionar colunas 'year' e 'month' extraídas de 'data_fechamento'
    df_agrupado = df_agrupado.withColumn("year", year(col("data_fechamento")).cast(StringType()))
    df_agrupado = df_agrupado.withColumn("month", month(col("data_fechamento")).cast(StringType()))

    # Filtragem para remover valores nulos e negativos
    df_agrupado = df_agrupado.filter(col("valor_pagamento") > 0)

    # Renomear colunas
    df_agrupado = df_agrupado.withColumnRenamed("valor_pagamento", "valor_retorno") 
    #                         .withColumnRenamed("ID_Registro_VX", "id_registro")

    # Ajustar formatação das colunas
    df_agrupado = df_agrupado.withColumn("data_fechamento", date_format(col("data_fechamento"), "yyyy-MM-dd")) \
                             .withColumn("data_lancamento", date_format(col("data_lancamento"), "yyyy-MM-dd")) \
                             .withColumn("id_registro", col("id_registro").cast(StringType())) \
                             .withColumn("valor_retorno", col("valor_retorno").cast("double"))

    # Selecionar colunas finais na ordem correta
    colunas_finais = ["data_fechamento", "data_ref", "id_registro", "valor_retorno", "data_lancamento", "year", "month", "atualizado_em"]
    df_agrupado = df_agrupado.select(*colunas_finais)

    # Exibir soma total de 'valor_retorno'
    df_agrupado.agg({"valor_retorno": "sum"}).show()
    
    # Exibir dataframe Final
    df_agrupado.show()

    # Colocando configs refined
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_REFINED_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_REFINED_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_REFINED_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")

    # Salvando os dados
    print("Iniciando salvamento dos arquivos")
    df_agrupado.write \
        .partitionBy("year", "month") \
        .format("delta") \
        .option("mergeSchema", "true") \
        .option("encoding", 'latin1') \
        .mode("overwrite") \
        .save("s3a://hemera-consolidado/retorno_consolidado")

    print("Arquivos Salvos")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("retorno_consolidado") \
        .config("spark.sql.encoding", "latin1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    hemera_raw_to_trusted_retorno(spark)
