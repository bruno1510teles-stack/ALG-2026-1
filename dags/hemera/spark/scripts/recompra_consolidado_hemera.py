from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import lit, coalesce, col, to_date, lpad, format_number, substring, date_format,year,month, expr, sum, max
from pyspark.sql.types import DoubleType
import os
from datetime import datetime, timezone, timedelta
import re

def recompra_consolidado(spark):
    # Colocando Configs
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_TRUSTED_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_TRUSTED_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_TRUSTED_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")

    # Lendo arquivos
    print("Iniciando leitura dos arquivos")
    
    nome_bucket = "hemera-diario"
    caminho = "recompra_diaria"
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
    df_consolidado.printSchema()
    df_consolidado.show(5)



    # Selecionar colunas desejadas
    colunas_desejadas = ['cnpj_cedente', 'nome_cedente', 'cnpj_sacado', 'nome_sacado',
        'id_titulo', 'tipo_titulo', 'data_emissao', 'data_aquisicao',
        'data_vencimento', 'numero_boleto_banco', 'valor_pagamento', 'data_arquivo', 'data_lancamento', 'data_ref']
    df_consolidado = df_consolidado.select(colunas_desejadas)

    # Filtrar valores maiores que zero
    df_consolidado = df_consolidado.filter(col("valor_pagamento") > 0)

    # Criar coluna 'data_fechamento'
    df_consolidado = df_consolidado.withColumn("data_fechamento", expr("last_day(data_arquivo)"))

    # Reorganizar colunas
    colunas = ["data_fechamento"] + [c for c in df_consolidado.columns if c != "data_fechamento"]
    df_consolidado = df_consolidado.select(colunas)

    # Converter colunas para datetime e manter apenas a data
    for coluna in ["data_vencimento", "data_emissao", "data_aquisicao", "data_fechamento", "data_lancamento"]:
        df_consolidado = df_consolidado.withColumn(coluna, date_format(col(coluna), "yyyy-MM-dd"))

    # Agrupar dados
    df_agrupado = df_consolidado.groupBy("data_ref", "id_titulo", "data_fechamento").agg(
        sum("valor_pagamento").alias("valor_pagamento"),
        max("data_lancamento").alias("data_lancamento")
    )

    # Adicionar timestamp de atualização
    now = datetime.now(tz=timezone(timedelta(hours=-3))).strftime('%Y-%m-%d %X')
    df_agrupado = df_agrupado.withColumn("atualizado_em", lit(now))

    # Adicionar colunas de ano e mês
    df_agrupado = df_agrupado.withColumn("year", year(col("data_fechamento")))
    df_agrupado = df_agrupado.withColumn("month", month(col("data_fechamento")))

    # Renomear colunas
    df_agrupado = df_agrupado.withColumnRenamed("valor_pagamento", "valor_recompra")
    #df_agrupado = df_agrupado.withColumnRenamed("IdTituloVx", "id_titulo")

    # Selecionar colunas finais
    colunas_finais = ['data_fechamento', 'id_titulo', 'valor_recompra', 'data_lancamento', 'year' ,'month' ,'data_ref', 'atualizado_em']
    df_agrupado = df_agrupado.select(colunas_finais)

    # Ajustar formatos de saída
    df_agrupado = df_agrupado.withColumn("data_fechamento", date_format(col("data_fechamento"), "yyyy-MM-dd"))
    df_agrupado = df_agrupado.withColumn("data_lancamento", date_format(col("data_lancamento"), "yyyy-MM-dd"))
    df_agrupado = df_agrupado.withColumn("id_titulo", lpad(col("id_titulo").cast("string"), 10, "0"))
    df_agrupado = df_agrupado.withColumn("valor_recompra", col("valor_recompra").cast("double"))
    df_agrupado = df_agrupado.withColumn("year", col("year").cast("string"))
    df_agrupado = df_agrupado.withColumn("month", col("month").cast("string"))
    df_agrupado = df_agrupado.withColumn("data_ref", col("data_ref").cast("string"))
    df_agrupado = df_agrupado.withColumn("atualizado_em", date_format(col("atualizado_em"), "yyyy-MM-dd HH:mm:ss"))

    # Exibir resultado
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
        .save("s3a://hemera-consolidado/recompra_consolidado")

    print("Arquivos Salvos")

    spark.stop()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("recompra_consolidado") \
        .config("spark.sql.encoding", "latin1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    recompra_consolidado(spark)
