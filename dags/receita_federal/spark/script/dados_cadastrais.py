from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when, to_date, date_format, current_timestamp, regexp_replace
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import os
from datetime import datetime, timezone, timedelta
from minio import Minio


def dados_cadastrais_to_refined(spark):

    # Limpar cache de metadados antes da leitura dos dados
    spark.catalog.clearCache()

    # Forçar o Delta a não reutilizar metadados antigos
    spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")

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

    # Leitura dos dados brutos da camada Trusted
    estabelecimentos = spark.read.format("delta").load("s3a://bureaus/receita-federal/estabelecimentos")\
            .withColumn("data_situacao_cadastral", col("data_situacao_cadastral").cast("date"))
    empresas = spark.read.format("delta").load("s3a://bureaus/receita-federal/empresas")\
            .withColumn("capital_social_empresa", col("capital_social_empresa").cast("double"))
    simples = spark.read.format("delta").load("s3a://bureaus/receita-federal/simples")
    natureza = spark.read.format("delta").load("s3a://bureaus/receita-federal/naturezas")
    cnae = spark.read.format("delta").load("s3a://bureaus/receita-federal/cnaes")
    municipio = spark.read.format("delta").load("s3a://bureaus/receita-federal/municipios")
    socios = spark.read.format("delta").load("s3a://bureaus/receita-federal/socios") \
        .withColumnRenamed("nome/razao_social", "nome_razao_social")
    qualificacoes = spark.read.format("delta").load("s3a://bureaus/receita-federal/qualificacoes")
    pep = spark.read.format("delta").load("s3a://pessoas-e-organizacoes/pep")
    print("Arquivos lidos")

        # Ajuste do formato dos documentos
    pep = pep.withColumn("documento", regexp_replace(col("documento"), "[^0-9]", ""))
    socios = socios.withColumn("documento_socio", regexp_replace(col("documento_socio"), "[^0-9]", ""))

    # Criando uma janela para pegar o sócio mais recente por empresa
    window_spec = Window.partitionBy("cnpj_raiz").orderBy(col("data_entrada_sociedade").desc())
    socios = socios.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).drop("row_num")

    # Criando uma janela para pegar o registro mais recente por documento na tabela PEP
    window_spec_pep = Window.partitionBy("documento").orderBy(col("data_ref").desc())
    pep = pep.withColumn("row_num_pep", row_number().over(window_spec_pep)).filter(col("row_num_pep") == 1).drop("row_num_pep")
    

     #Criação da tabela final
    dados_cadastrais = (
    estabelecimentos.alias("e")
    .join(socios.alias("soci"), col("e.cnpj_raiz") == col("soci.cnpj_raiz"), "left")
    .join(qualificacoes.alias("q"), col("soci.qualificacao_socio") == col("q.codigo"), "left")
    .join(empresas.alias("emp"), col("e.cnpj_raiz") == col("emp.cnpj_raiz"), "left")
    .join(simples.alias("s"), col("e.cnpj_raiz") == col("s.cnpj_raiz"), "left")
    .join(cnae.alias("cnae"), col("e.cnae_principal") == col("cnae.codigo"), "left")
    .join(natureza.alias("nat"), col("emp.natureza_juridica") == col("nat.codigo"), "left")
    .join(municipio.alias("m"), col("e.municipio") == col("m.codigo"), "left")
    .join(pep.alias("pep"), (col("soci.documento_socio") == col("pep.documento")) &
                             (col("soci.nome_razao_social") == col("pep.nome")), "left")
    .select(
        # Informações da empresa
        col("e.cnpj_raiz"),
        col("e.documento_sem_formatacao").alias("cnpj_sem_formatacao"),
        col("e.documento_formatado").alias("cnpj_formatado"),
        when(col("e.is_matriz") == True, "Sim").otherwise("Nao").alias("flag_matriz"),
        col("emp.razao_social"),
        col("e.nome_fantasia"),
        col("e.situacao_cadastral"),
        to_date(col("e.data_situacao_cadastral"), "yyyy-MM-dd").alias("data_situacao_cadastral"),
        to_date(col("e.data_inicio_atividade"), "yyyy-MM-dd").alias("data_fundacao"),
        col("e.cnae_principal").alias("cnae_principal_codigo"),
        col("cnae.descricao").alias("cnae_principal_descricao"),
        col("e.logradouro"),
        col("e.numero"),
        col("e.complemento"),
        col("e.bairro"),
        col("e.cep"),
        col("e.uf"),
        col("m.descricao").alias("municipio"),
        col("e.situacao_especial"), 
        to_date(col("e.data_sitaucao_especial"), "yyyy-MM-dd").alias("data_situacao_especial"),
        col("emp.natureza_juridica").alias("natureza_juridica_codigo"),
        col("nat.descricao").alias("descricao_natureza_juridica"),
        col("emp.porte_empresa"),
        col("emp.capital_social_empresa").cast("double").alias("capital_social_empresa"),
        when(col("s.is_simples") == True, "Sim").otherwise("Nao").alias("flag_simples"),
        to_date(col("s.data_opcao_pelo_simples"), "yyyy-MM-dd").alias("data_opcao_pelo_simples"),
        to_date(col("s.data_exclusao_simples"), "yyyy-MM-dd").alias("data_exclusao_simples"),
        when(col("s.is_mei") == True, "Sim").otherwise("Nao").alias("is_mei"),
        to_date(col("s.data_opcao_pelo_mei"), "yyyy-MM-dd").alias("data_opcao_pelo_mei"),
        to_date(col("s.data_exclusao_mei"), "yyyy-MM-dd").alias("data_exclusao_mei"),
        
        # Informações dos sócios
        col("soci.identificador_socio").alias("tipo_socio"),
        col("soci.documento_socio").alias("documento_socio_mais_recente"),
        col("soci.nome_razao_social").alias("socio_nome"),
        col("soci.qualificacao_socio").alias("socio_qualificacao_codigo"),
        col("q.descricao").alias("socio_qualificacao_descricao"),
        to_date(col("soci.data_entrada_sociedade"), "yyyy-MM-dd").alias("data_entrada_sociedade"),
        
        # Informações PEP
        col("pep.nome").alias("pep_nome"),
        col("pep.funcao").alias("pep_funcao"),
        col("pep.nome_orgao").alias("pep_nome_orgao"),
        to_date(col("pep.data_inicio_exercicio"), "yyyy-MM-dd").alias("data_inicio_exercicio"),
        to_date(col("pep.data_fim_exercicio"), "yyyy-MM-dd").alias("data_fim_exercicio"),
        to_date(col("pep.data_fim_carencia"), "yyyy-MM-dd").alias("data_fim_carencia"),
        
        # Outras informações
        col("e.data_ref").alias("data_ref"),
        date_format(current_timestamp(), "yyyy-MM-dd").alias("Atualizado_em")
    )
)

    # Lista de colunas esperadas na ordem correta
    colunas_ordenadas = [
        # Informações da empresa
        "cnpj_raiz",
        "cnpj_sem_formatacao",
        "cnpj_formatado",
        "flag_matriz",
        "razao_social",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "data_fundacao",
        "cnae_principal_codigo",
        "cnae_principal_descricao",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        "situacao_especial",
        "data_situacao_especial",
        "natureza_juridica_codigo",
        "descricao_natureza_juridica",
        "porte_empresa",
        "capital_social_empresa",
        "flag_simples",
        "data_opcao_pelo_simples",
        "data_exclusao_simples",
        "is_mei",
        "data_opcao_pelo_mei",
        "data_exclusao_mei",

        # Informações dos sócios
        "tipo_socio",
        "documento_socio_mais_recente",
        "socio_nome_razao_social",
        "socio_qualificacao_codigo",
        "socio_qualificacao_descricao",
        "data_entrada_sociedade",

        # Informações PEP
        "pep_nome",
        "pep_funcao",
        "pep_nome_orgao",
        "data_inicio_exercicio",
        "data_fim_exercicio",
        "data_fim_carencia",

        # Outras informações
        "data_ref",
        "Atualizado_em"
    ]

    # Verifica se todas as colunas existem no DataFrame
    colunas_existentes = [col for col in colunas_ordenadas if col in dados_cadastrais.columns]

    # Reorganiza o DataFrame apenas com as colunas existentes
    dados_cadastrais = dados_cadastrais.select(colunas_existentes)

    # Salvando arquivos
    hadoop_conf.set("fs.s3a.access.key", os.getenv('MINIO_REFINED_ACCESS_KEY'))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv('MINIO_REFINED_SECRET_KEY'))
    hadoop_conf.set("fs.s3a.endpoint", os.getenv('MINIO_REFINED_ENDPOINT'))
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
    hadoop_conf.set("fs.s3a.path.style.access", "true")

    print("Iniciando salvamento dos arquivos")
    dados_cadastrais.write \
        .partitionBy("data_ref") \
        .format("delta") \
        .option("mergeSchema", "true") \
        .option("encoding", 'latin1') \
        .mode("overwrite") \
        .save(f"s3a://receita-federal/dados-cadastrais") 

    print("Arquivos Salvos")

    spark.stop()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("PrefiltroToRefined") \
        .config("spark.sql.encoding", "latin1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite","LEGACY") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    dados_cadastrais_to_refined(spark)
