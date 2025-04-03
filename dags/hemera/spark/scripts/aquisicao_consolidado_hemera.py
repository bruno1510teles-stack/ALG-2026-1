from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import trim,lit, coalesce, col, to_date, when, input_file_name, regexp_extract, substring, last_day, date_format, StringType, sum, max, year, month, expr, regexp_replace
from pyspark.sql.types import DoubleType, DateType, DecimalType
from datetime import datetime, timezone, timedelta
import re
import os

def aquisicao_consolidado(spark, **kwargs):
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
    caminho = "aquisicao_diaria"
    anos = ['2024','2025']
    meses = ['str(m).zfill(2) for m in range(1, 13)']

    df_list = []

    for ano in anos:
        for mes in meses:
            caminho_csv = f"s3a://{nome_bucket}/{caminho}/year={ano}/month={mes}/"

            try:
                df_ano = spark.read.format("delta").load(caminho_csv)

                # Adicionar a coluna 'data_arquivo' com o último dia do mês
                data_arquivo = datetime(int(ano), int(mes), 1).date()
                df_ano = df_ano.withColumn("data_arquivo", lit(data_arquivo))

                # Adicionar a coluna 'data_ref' no formato "YYYY-MM"
                data_ref = f"{ano}-{mes.zfill(2)}"  # Garantir que o mês tenha dois dígitos
                df_ano = df_ano.withColumn("data_ref", lit(data_ref))

                df_list.append(df_ano)
                print(f"Arquivos lidos para {ano}/{mes}")

            except Exception as e:
                print(f"Erro ao ler arquivos para {ano}/{mes}: {e}")

    # Função para extrair a data do nome do arquivo
    def extract_date_from_filename(filename):
        date_match = re.search(r'(\d{2}\.\d{2}\.\d{2})', filename)
        if date_match:
            return datetime.strptime(date_match.group(1), '%d.%m.%y').date()
        return None

    print("Finalizado a leitura dos arquivos")

    if df_list:
        df_consolidado = df_list[0]
        for df_atual in df_list[1:]:
            df_consolidado = df_consolidado.unionByName(df_atual, allowMissingColumns=True)
    else:
        print("Nenhum arquivo encontrado. Encerrando a função.")
        df_consolidado = None

    print("Arquivos Lidos")

    # Criar a coluna 'data_fechamento'
    # Garantir que 'data_ref' tenha valores distintos
    df_consolidado.select('data_ref').distinct().show()

    # Criar a coluna data_fechamento corretamente
    df_consolidado = df_consolidado.withColumn("data_fechamento", expr("last_day(data_arquivo)"))

    # Converter colunas para DateType
    for coluna in ["data_vencimento", "data_emissao", "data_aquisicao", "data_fechamento"]:
        df_consolidado = df_consolidado.withColumn(coluna, col(coluna).cast(DateType()))

    # Criar coluna 'atualizado_em'
    now = datetime.now(timezone(timedelta(hours=-3))).strftime("%Y-%m-%d %H:%M:%S")
    df_consolidado = df_consolidado.withColumn("atualizado_em", lit(now).cast(StringType()))

    # Criar colunas 'year' e 'month'
    df_consolidado = df_consolidado.withColumn("year", expr("year(data_fechamento)").cast(StringType()))
    df_consolidado = df_consolidado.withColumn("month", expr("month(data_fechamento)").cast(StringType()))

    # Garantir que 'id_titulo' e 'data_ref' são strings corretamente
    df_consolidado = df_consolidado.withColumn("id_titulo", col("id_titulo").cast(StringType()))
    df_consolidado = df_consolidado.withColumn("data_ref", col("data_ref").cast(StringType()))

    # Converter 'valor_aquisicao' para DecimalType (corrigindo possível erro de vírgula)
    df_consolidado = df_consolidado.withColumn(
        "valor_aquisicao",
        regexp_replace(col("valor_aquisicao"), ",", ".").cast(DecimalType(10, 2))
    )

    colunas_finais = ["data_fechamento", "data_ref", "id_titulo", "valor_aquisicao", "year", "month", "atualizado_em"]

    df_final = df_consolidado.select(*colunas_finais)

    # Formatar as colunas conforme o script em Pandas:
    df_final = df_final.withColumn("data_fechamento", date_format(col("data_fechamento"), "yyyy-MM-dd"))
    df_final = df_final.withColumn("id_titulo", F.expr("lpad(id_titulo, 10, '0')"))
    df_final = df_final.withColumn("valor_aquisicao", col("valor_aquisicao").cast(DecimalType(10, 2)))
    df_final = df_final.withColumn("atualizado_em", date_format(col("atualizado_em"), "yyyy-MM-dd HH:mm:ss"))

    # Mostrar resultado final
    df_final.show()

    # Colocando configs refined
    hadoop_conf.unset("fs.s3a.access.key")
    hadoop_conf.unset("fs.s3a.secret.key")
    hadoop_conf.unset("fs.s3a.endpoint")

    # Colocando configs refined
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_REFINED_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_REFINED_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_REFINED_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")

    # Salvando os dados
    print("Iniciando salvamento dos arquivos")
    df_final.write \
        .partitionBy("year", "month") \
        .format("delta") \
        .option("mergeSchema", "true") \
        .option("encoding", 'utf-8') \
        .mode("overwrite") \
        .save("s3a://hemera-consolidado/aquisicao_consolidado/")

    print("Arquivos Salvos")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("aquisicao_consolidada") \
        .config("spark.sql.encoding", "latin1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    aquisicao_consolidado(spark)
