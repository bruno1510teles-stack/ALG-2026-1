from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import lit, concat, lpad, substring, coalesce, col, to_date, when, regexp_extract, input_file_name
from pyspark.sql.types import DoubleType, DateType, IntegerType
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
    print("Minio acessado")
    print("Iniciando leitura dos arquivos")

    # Lendo arquivos
    nome_bucket = "hemera-diario"
    caminho = 'estoque-csv'
    anos = ['2021','2022','2023','2024','2025']                         
    meses = [str(m).zfill(2) for m in range(1, 13)]
    
    df_list = []

    for ano in anos:
        for mes in meses:
            caminho_csv = f"s3a://{nome_bucket}/{caminho}/year={ano}/month={mes}/"
            

            try:
                df_ano = spark.read \
                    .option("delimiter", ",") \
                    .option("header", True) \
                    .option("allowMissingColumns", True) \
                    .option("encoding",'utf-8') \
                    .csv(caminho_csv)

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

    print("Arquivos lidos")

    if df_list:
        df = df_list[0]  
        for df_atual in df_list[1:]:
            df = df.unionByName(df_atual, allowMissingColumns=True)
    else:
        print("Nenhum arquivo encontrado. Encerrando a função.")
        return None 


    print("Iniciando Tratamento Dados")
    df = df.filter((col("DataPosicao").isNotNull()) & (col("DataPosicao") != ""))
    padrao_data = r'^\d{4}-\d{2}-\d{2}$'
    df = df.filter(F.col("DataPosicao").rlike(padrao_data))
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

    df.show(5)

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

    # Tratando valor_nominal
    if "ValorNominal" in df.columns and "ValorNominalOriginal" in df.columns:
        df = df.withColumn("valor_nominal", coalesce(col("ValorNominal"), col("ValorNominalOriginal")))
    elif "ValorNominal" in df.columns:
        df = df.withColumn("valor_nominal", col("ValorNominal"))
    elif "ValorNominalOriginal" in df.columns:
        df = df.withColumn("valor_nominal", col("ValorNominalOriginal"))    

    df = df.withColumn(
        "produto",
        F.when(F.col("CedenteCnpjCpf") == '28.494.032/0001-00', 'VENDERMAIS')
        .when(F.col("CedenteCnpjCpf") == '35.914.008/0001-48', 'BLIPS')
        .otherwise('TRADICIONAL')
    )

    # Extraindo a data do nome do arquivo (formato DD.MM.YY)
    regex_data = r"(\d{2}\.\d{2}\.\d{2})"
    df = df.withColumn("data_arquivo_extraida", regexp_extract(input_file_name(), regex_data, 1))

    # Renomeando a coluna 'data_arquivo' apenas se necessário para evitar duplicação
    df = df.withColumnRenamed("DataPosicao", "data_arquivo")

    # Convertendo a string da data para o formato correto (YYYY-MM-DD)
    df = df.withColumn(
        "data_arquivo_extraida", 
        to_date(col("data_arquivo_extraida"), "dd.MM.yy")
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
                        "data_arquivo","data_referencia", "atualizado_em"]

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
        .withColumn("data_ocorrencia_prorrogacao", to_date(substring(col("data_ocorrencia_prorrogacao"), 1, 10), "yyyy-MM-dd"))
        

    # Formatar as colunas de valores numéricos como DoubleType
    df = df.withColumn("valor_aquisicao", col("valor_aquisicao").cast(DoubleType())) \
        .withColumn("valor_nominal", col("valor_nominal").cast(DoubleType())) \
        .withColumn("valor_presente", col("valor_presente").cast(DoubleType())) \
        .withColumn("pdd_nota", col("pdd_nota").cast(DoubleType())) \
        .withColumn("pdd_vencido", col("pdd_vencido").cast(IntegerType()))

    
    print("Tratamento Concluído")

    # Criando colunas 'year' e 'month' baseadas na 'data_arquivo'
    df = df.withColumn("year", substring(col("data_arquivo"), 1, 4))  
    df = df.withColumn("month", substring(col("data_arquivo"), 6, 2)) 

    # Mostrando as primeiras linhas
    print("dataframe final")
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
        .partitionBy("year","month") \
        .format("delta") \
        .option("mergeSchema", "true") \
        .option("encoding", 'latin1') \
        .mode("overwrite") \
        .save("s3a://hemera-diario/estoque_diaria")

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