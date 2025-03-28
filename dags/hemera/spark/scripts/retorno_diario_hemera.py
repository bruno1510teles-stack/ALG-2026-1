from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import lit, coalesce, col, to_date, when, input_file_name, regexp_extract, substring, trim, regexp_replace
from pyspark.sql.types import DoubleType, DateType
from datetime import datetime, timezone, timedelta
from io import BytesIO
import re
import os

def retorno_diaria(spark):
    # Configuração do Hadoop para acessar o S3 (MinIO)
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_RAW_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_RAW_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_RAW_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    print("MInio ACessado")

    nome_bucket = "hemera-csv"
    caminho = "retorno-csv"
    anos = ['2025','2024']
    meses = [str(m).zfill(2) for m in range(1, 13)]

    df_list = []

    for ano in anos:
        for mes in meses:
            caminho_csv = f"s3a://{nome_bucket}/{caminho}/year={ano}/month={mes}/"
            

            try:
                df_ano = spark.read \
                    .option("delimiter", ";") \
                    .option("header", True) \
                    .option("allowMissingColumns", True) \
                    .option("encoding",'cp1252') \
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

    df.show(5)
    df.printSchema()


###########################################################
#####################  TRATAMENTOS ########################
###########################################################
    print("Iniciando os tratamentos")

    # Ajustando o DE - PARA do campo CedenteCnpjCPF
    df = df.withColumn(
    "produto",
    when(col("CedenteCnpjCpf") == '28.494.032/0001-00', 'VENDERMAIS')
    .when(col("CedenteCnpjCpf") == '35.914.008/0001-48', 'BLIPS')
    .otherwise('TRADICIONAL')
)

    # Extraindo a data do nome do arquivo (formato DD.MM.YY)
    regex_data = r"(\d{2}\.\d{2}\.\d{2})"
    df = df.withColumn("data_arquivo", regexp_extract(input_file_name(), regex_data, 1))
    df = df.withColumn("data_arquivo", to_date(df["data_arquivo"], "dd.MM.yy"))

    # Renomeando colunas
    colunas_para_renomear = { 
        "Ocorrencia":"ocorrencia",
        "Situacao":"situacao",
        "CedenteCnpjCpf":"cnpj_cedente",     
        "CedenteNome":"nome_cedente",      
        "SacadoCnpjCpf":"cnpj_sacado",     
        "SacadoNome":"nome_sacado",
        "TipoAtivo":"tipo_ativo",     
        "ID_Registro_VX":"id_registro",               
        "DataVencimento":"data_vencimento",     
        "NumeroBoletoBanco":"numero_boleto_banco",     
        "NumeroTitulo":"numero_titulo",     
        "CampoChave":"campo_chave",     
        "ValorNominal":"valor_nominal",
        "ValorPagamento":"valor_pagamento",          
        "Coobrigacao":"coobrigacao"
    }

    for coluna_antiga, nova_coluna in colunas_para_renomear.items():
        if coluna_antiga in df.columns:
            df = df.withColumnRenamed(coluna_antiga, nova_coluna)

    # Removendo espaços extras
    df = df.withColumn("data_vencimento", trim(col("data_vencimento")))
    df = df.withColumn("valor_nominal", trim(col("valor_nominal")))
    df = df.withColumn("valor_pagamento", trim(col("valor_pagamento")))

    # Convertendo colunas para date
    df = df.withColumn("data_vencimento", to_date(col("data_vencimento"), "dd/MM/yyyy"))

    # Convertendo colunas para double, removendo vírgulas
    df = df.withColumn("valor_nominal", regexp_replace(col("valor_nominal"), ",", ".").cast("double"))
    df = df.withColumn("valor_pagamento", regexp_replace(col("valor_pagamento"), ",", ".").cast("double"))

    # Inserindo coluna atualizado_em
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    atualizado_em = now.strftime('%Y-%m-%d %X')  
    df = df.withColumn("atualizado_em", lit(atualizado_em))

    # Lista de colunas desejadas
    colunas_desejadas = ["ocorrencia", "situacao", "cnpj_cedente", "nome_cedente", "cnpj_sacado", "nome_sacado","produto","tipo_ativo","id_registro",
                        "data_vencimento", "numero_boleto_banco", "numero_titulo", "campo_chave", "valor_nominal","valor_pagamento","Abatimentos",
                        "Juros","data_arquivo","atualizado_em"
                        ]

    # Verificar se cada coluna existe, e se não existir, adicioná-la com valor None
    for coluna in colunas_desejadas:
        if coluna not in df.columns:
            df = df.withColumn(coluna, lit(None))

    # Selecionar as colunas
    df = df.select(*colunas_desejadas)

    # Exibir resultados
    df.show(5)
    df.printSchema()

    print("Tratamento concluído")

    # Criando colunas 'year' e 'month' baseadas na 'data_arquivo'
    df = df.withColumn("year", substring(col("data_arquivo"), 1, 4))  
    df = df.withColumn("month", substring(col("data_arquivo"), 6, 2)) 


    # Tirando configs da raw
    hadoop_conf.unset("fs.s3a.access.key")
    hadoop_conf.unset("fs.s3a.secret.key")
    hadoop_conf.unset("fs.s3a.endpoint")

    # Colocando Configs
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_TRUSTED_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_TRUSTED_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_TRUSTED_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")


    print("Iniciando salvamento dos arquivos na TRUSTED")
    # Salvando com o particionamento correto
    df.write \
        .partitionBy("year", "month") \
        .format("delta") \
        .option("mergeSchema", "true") \
        .option("encoding", 'latin1') \
        .mode("overwrite") \
        .save(f"s3a://hemera-diario/retorno_diaria")

    print("Arquivos Salvos")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("retorno_diaria") \
        .config("spark.sql.encoding", "latin1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df_final = retorno_diaria(spark)
