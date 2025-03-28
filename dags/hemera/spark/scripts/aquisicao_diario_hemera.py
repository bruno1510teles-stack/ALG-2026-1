from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import lit, coalesce, col, to_date, when, input_file_name, regexp_extract, substring
from pyspark.sql.types import DoubleType, DateType
from datetime import datetime, timezone, timedelta
from io import BytesIO
import re
import os 

def aquisicao_diaria(spark):
    # Configuração do Hadoop para acessar o S3 (MinIO)
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_RAW_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_RAW_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_RAW_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")

    nome_bucket = "hemera-csv"
    caminho = "aquisicao-csv"
    anos = ['2024','2025']
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

    print("Iniciando os tratamentos")

    # Criar novas colunas combinadas
    if "ValorNominal" in df.columns:
        df = df.withColumn("valor_nominal", col("ValorNominal"))

    # Ajustando o DE - PARA do campo CedenteCnpjCPF
    df = df.withColumn(
        "produto",
        when(col("CedenteCnpjCpf") == '28.494.032/0001-00', 'VENDERMAIS')
        .when(col("CedenteCnpjCpf") == '35.914.008/0001-48', 'BLIPS')
        .otherwise('TRADICIONAL')
    )

    # Extraindo a data do nome do arquivo (formato DD.MM.YY)
    regex_data = r"(\d{2}\.\d{2}\.\d{2})"

    # Adicionando a coluna com a data extraída do nome do arquivo
    df = df.withColumn("data_arquivo", regexp_extract(input_file_name(), regex_data, 1))

    # Convertendo a string da data para o formato correto (YYYY-MM-DD)
    df = df.withColumn("data_arquivo", to_date(df["data_arquivo"], "dd.MM.yy"))

    # Renomeando colunas
    colunas_para_renomear = {     
        "CedenteCnpjCpf":"cnpj_cedente",     
        "CedenteNome":"nome_cedente",      
        "SacadoCnpjCpf":"cnpj_sacado",     
        "SacadoNome":"nome_sacado",     
        "IdTituloVx":"id_titulo",     
        "TipoAtivo":"tipo_titulo",     
        "DataEmissao":"data_emissao",     
        "DataAquisicao1":"data_aquisicao",     
        "DataVencimento":"data_vencimento",     
        "NumeroBoletoBanco":"numero_boleto_banco",     
        "NumeroTitulo":"numero_titulo",     
        "CampoChave":"campo_chave",     
        "ValorAquisicao":"valor_aquisicao",             
        "Coobrigacao":"coobrigacao"
    }


    # Convertendo colunas para date
    date_columns = ["data_emissao", "data_aquisicao", "data_vencimento"]

    # Convertendo colunas para double
    double_columns = [
        "valor_aquisicao", "ValorNominal"
    ]
    
    for col_name in double_columns:
        if col_name in df.columns: 
            df = df.withColumn(col_name, col(col_name).cast(DoubleType()))

    for col_name in date_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, to_date(col(col_name), "yyyy-MM-dd"))

    for coluna_antiga, nova_coluna in colunas_para_renomear.items():
        if coluna_antiga in df.columns:
            df = df.withColumnRenamed(coluna_antiga, nova_coluna)

    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    atualizado_em = now.strftime('%Y-%m-%d %X')  
    df = df.withColumn("atualizado_em", lit(atualizado_em))

    # Lista de colunas desejadas
    colunas_desejadas = ["cnpj_cedente", "nome_cedente", "cnpj_sacado", "nome_sacado","produto","id_titulo", 
                        "tipo_titulo", "data_emissao", "data_aquisicao", "data_vencimento", "numero_boleto_banco", 
                        "numero_titulo", "campo_chave", "valor_aquisicao","valor_nominal","coobrigacao","Taxa",
                        "data_arquivo","atualizado_em"
                        ]

    # Verificar se cada coluna existe, e se não existir, adicioná-la com valor None
    for coluna in colunas_desejadas:
        if coluna not in df.columns:
            df = df.withColumn(coluna, lit(None))

    # Selecionar as colunas
    df = df.select(*colunas_desejadas)

    print("Tratamento concluido")
    
    df.show(5)

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
        .save(f"s3a://hemera-diario/aquisicao_diaria")

    print("Arquivos Salvos")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("aquisicao_diaria") \
        .config("spark.sql.encoding", "latin1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df_final = aquisicao_diaria(spark)
