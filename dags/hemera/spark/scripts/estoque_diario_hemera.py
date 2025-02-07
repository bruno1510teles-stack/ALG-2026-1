from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import lit, concat, lpad, substring, coalesce, col, to_date, when
from pyspark.sql.types import DoubleType, DateType
import os
from datetime import datetime, timezone, timedelta


def estoque_diario_hemera(spark):
    # Colocando Configs
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

    # Lendo arquivos
    bucket_name = "hemera"
    caminho = 'estoque_csv'

    # Definir o caminho de cada ano
    anos = ['2024', '2023', '2022', '2021']
    meses = ['01',	'02',	'03',	'04',	'05',	'06',	'07',	'08',	'09',	'10',	'11',	'12']

    # Lista para armazenar os DataFrames de cada ano
    df_list = []
    for ano in anos:
        for mes in meses:
            caminho_csv = f"s3a://{bucket_name}/{caminho}/year={ano}/month={mes}/"

            try:
                # Tentar ler o arquivo
                df_ano = spark.read \
                    .option("delimiter", ",") \
                    .option("header", True) \
                    .option("allowMissingColumns", True) \
                    .csv(caminho_csv)

                # Adicionar o DataFrame do ano à lista
                df_list.append(df_ano)
                print(f"Arquivos lidos para {ano}/{mes}")
            
            except Exception as e:
                print(f"Erro ao ler arquivos para {ano}/{mes}: {e}")

    # Verificar se a lista contém DataFrames antes de tentar unir
    if df_list:
        # Unir os DataFrames de todos os anos
        df = df_list[0]  
        for df_atual in df_list[1:]:
            df = df.unionByName(df_atual, allowMissingColumns=True)
    else:
        df = None
        print("Nenhum arquivo foi encontrado para os anos e meses fornecidos.")

    print("Arquivos Lidos")

    print("Iniciando Tratamento Dados")
    df = df.filter((col("DataPosicao").isNotNull()) & (col("DataPosicao") != ""))
    # Função para converter números de data do Excel para o formato de data
    def excel_date_to_date(excel_date):
        return F.from_unixtime((F.col(excel_date) - 25569) * 86400).cast(DateType())

    # Tratar a coluna "DataPosicao" para lidar com valores no formato Excel e já formatados
    df = df.withColumn(
        "DataPosicao",
        F.when(F.col("DataPosicao").cast("int").isNotNull(), excel_date_to_date("DataPosicao"))
        .otherwise(F.col("DataPosicao"))
    )

    # Adicionando coluna 'data_ref'
    df = df.withColumn("data_referencia", 
                    concat(substring(df['DataPosicao'], 1, 4), lit('-'), 
                            lpad(substring(df['DataPosicao'], 6, 2), 2, '0')))  

    # Tratando pdd_nota
    if "PDD NOTA" in df.columns and "PDDNota" in df.columns:
        df = df.withColumn("pdd_nota", coalesce(col("PDD NOTA"), col("PDDNota")))
    elif "PDD NOTA" in df.columns:
        df = df.withColumn("pdd_nota", col("PDD NOTA"))
    elif "PDDNota" in df.columns:
        df = df.withColumn("pdd_nota", col("PDDNota"))

    # Tratando pdd_vencido
    if "PDD REGULAMENTO" in df.columns and "PDDVencido" in df.columns:
        df = df.withColumn("pdd_vencido", coalesce(col("PDD REGULAMENTO"), col("PDDVencido")))
    elif "PDD REGULAMENTO" in df.columns:
        df = df.withColumn("pdd_vencido", col("PDD REGULAMENTO"))
    elif "PDDVencido" in df.columns:
        df = df.withColumn("pdd_vencido", col("PDDVencido"))    

    df = df.withColumn(
        "produto",
        F.when(F.col("CedenteCnpjCpf") == '28.494.032/0001-00', 'VENDERMAIS')
        .when(F.col("CedenteCnpjCpf") == '35.914.008/0001-48', 'BLIPS')
        .otherwise('TRADICIONAL')
    )

    # Renomeando colunas
    colunas_para_renomear = {
        "Situacao":"situacao",	 
        "CedenteTipoInscricao":"tipo_cedente",	 
        "CedenteCnpjCpf":"cnpj_cedente",	 
        "CedenteNome":"nome_cedente",	 
        "SacadoTipoInscricao":"tipo_sacado",	 
        "SacadoCnpjCpf":"cnpj_sacado",	 
        "SacadoNome":"nome_sacado",	 
        "IdTituloVx":"id_titulo",	 
        "TipoAtivo":"tipo_titulo",	 
        "DataEmissao":"data_emissao",	 
        "DataAquisicao":"data_aquisicao",	 
        "DataVencimento":"data_vencimento",	 
        "NumeroBoletoBanco":"numero_boleto_banco",	 
        "NumeroTitulo":"numero_titulo",	 
        "CampoChave":"campo_chave",	 
        "ValorAquisicao":"valor_aquisicao",	 
        "ValorNominal":"valor_nominal",	 
        "ValorPresente":"valor_presente",	 
        "DataPosicao":"data_arquivo",	 
        "DataProrrogacao":"data_prorrogacao",	 
        "DataOcorrenciaProrrogacao":"data_ocorrencia_prorrogacao",	 
        "Coobrigacao":"coobrigacao",	 
        "OriginadorCpfCnpj":"cnpj_originador"
    }

    for coluna_antiga, nova_coluna in colunas_para_renomear.items():
        df = df.withColumnRenamed(coluna_antiga, nova_coluna)

    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    atualizado_em = now.strftime('%Y-%m-%d %X')  
    df = df.withColumn("atualizado_em", lit(atualizado_em))

    # Lista de colunas desejadas
    colunas_desejadas = ["cnpj_originador", "coobrigacao", "produto", "cnpj_cedente", "nome_cedente", "tipo_cedente", 
                        "cnpj_sacado", "nome_sacado", "tipo_sacado", "id_titulo", "numero_titulo", 
                        "numero_boleto_banco", "campo_chave", "tipo_titulo", "situacao", "data_emissao", 
                        "data_aquisicao", "data_vencimento", "data_prorrogacao", "data_ocorrencia_prorrogacao", 
                        "valor_aquisicao", "valor_nominal", "valor_presente", "pdd_nota", "pdd_vencido", 
                        "data_arquivo", "data_referencia", "atualizado_em"]

    # Verificar se cada coluna existe, e se não existir, adicioná-la com valor None
    for coluna in colunas_desejadas:
        if coluna not in df.columns:
            df = df.withColumn(coluna, lit(None))

    # Selecionar as colunas
    df = df.select(*colunas_desejadas)


    # Formatando colunas

    df = df.withColumn("data_emissao", to_date(substring(col("data_emissao"), 1, 10), "yyyy-MM-dd")) \
        .withColumn("data_aquisicao", to_date(substring(col("data_aquisicao"), 1, 10), "yyyy-MM-dd")) \
        .withColumn("data_vencimento", to_date(substring(col("data_vencimento"), 1, 10), "yyyy-MM-dd")) \
        .withColumn("data_prorrogacao", to_date(substring(col("data_prorrogacao"), 1, 10), "yyyy-MM-dd")) \
        .withColumn("data_ocorrencia_prorrogacao", to_date(substring(col("data_ocorrencia_prorrogacao"), 1, 10), "yyyy-MM-dd")) \
        .withColumn("data_arquivo", to_date(substring(col("data_arquivo"), 1, 10), "yyyy-MM-dd"))

    # Formatar as colunas de valores numéricos como DoubleType
    df = df.withColumn("valor_aquisicao", col("valor_aquisicao").cast(DoubleType())) \
        .withColumn("valor_nominal", col("valor_nominal").cast(DoubleType())) \
        .withColumn("valor_presente", col("valor_presente").cast(DoubleType())) \
        .withColumn("pdd_nota", col("pdd_nota").cast(DoubleType())) \
        .withColumn("pdd_vencido", col("pdd_vencido").cast(DoubleType()))

    padrao_data = r'^\d{4}-\d{2}-\d{2}$'
    df = df.filter(F.col("data_arquivo").rlike(padrao_data))
    
    print("Tratamento Concluído")

    # Mostrando as primeiras linhas
    df.show(10)

    # Tirando configs da raw
    hadoop_conf.unset("fs.s3a.access.key")
    hadoop_conf.unset("fs.s3a.secret.key")
    hadoop_conf.unset("fs.s3a.endpoint")

    # Colocando configs trusted
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_TRUSTED_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_TRUSTED_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_TRUSTED_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")

    print("Iniciando salvamento dos arquivos")
    df.write \
        .partitionBy("data_referencia", "data_arquivo") \
        .format("delta") \
        .option("mergeSchema", "true") \
        .option("encoding", 'latin1') \
        .mode("append") \
        .save("s3a://trusted-hemera/estoque_diario")

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
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    estoque_diario_hemera(spark)