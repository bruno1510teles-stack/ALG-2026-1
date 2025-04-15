from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType
from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, col, length, to_date, date_format, current_timestamp, regexp_replace, trim, first, translate
from pyspark.sql import functions as F
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
    pep = pep.withColumn("documento", F.regexp_replace(F.col("documento"), "[^0-9]", ""))
    socios = socios.withColumn("documento_socio", F.regexp_replace(F.col("documento_socio"), "[^0-9]", ""))
    
    # Criando uma janela para pegar o sócio mais recente por empresa
    window_spec = Window.partitionBy("cnpj_raiz").orderBy(F.col("data_entrada_sociedade").desc())
    socios_mais_recente = socios.withColumn("row_num", F.row_number().over(window_spec)).filter(F.col("row_num") == 1).drop("row_num")
    

    # Tratamento dos documentos: remove ponto e traço na base PEP
    pep = pep.withColumn("documento_tratado", F.regexp_replace(F.col("documento"), "[\\.\\-]", ""))

    # Padronização do nome para comparação (letras minúsculas)
    pep = pep.withColumn("nome_lower", F.lower(F.col("nome")))
    socios = socios.withColumn("nome_lower", F.lower(F.col("nome_razao_social")))

    # Join com base PEP usando documento OU nome (ambos tratados)
    socios_pep_joined = (
        socios.alias("s")
        .join(
            pep.alias("p"),
            (
                (F.col("s.documento_socio") == F.col("p.documento_tratado")) &
                (F.col("s.nome_lower") == F.col("p.nome_lower"))
            ),
            "left"
        )
    )

    # Coluna de flag de PEP
    socios_pep_joined = socios_pep_joined.withColumn("pep", F.when(F.col("p.nome").isNotNull(), "sim").otherwise("nao"))

    # Agora podemos filtrar os sócios que são PEP corretamente
    socios_pep = socios_pep_joined.filter(F.col("pep") == "sim").groupBy("cnpj_raiz").agg(
        F.first("s.nome_razao_social").alias("pep_nome"),
        F.first("p.funcao").alias("pep_funcao"),
        F.first("p.nome_orgao").alias("pep_nome_orgao"),
        F.first("p.documento").alias("pep_documento")
    )
    
    # Criando a tabela final com os ajustes
    dados_cadastrais = (
        estabelecimentos.alias("e")
        .join(socios_mais_recente.alias("soci"), F.col("e.cnpj_raiz") == F.col("soci.cnpj_raiz"), "left")
        .join(socios_pep.alias("pep_info"), F.col("e.cnpj_raiz") == F.col("pep_info.cnpj_raiz"), "left")  # Junta sócios PEP
        .join(qualificacoes.alias("q"), F.col("soci.qualificacao_socio") == F.col("q.codigo"), "left")
        .join(empresas.alias("emp"), F.col("e.cnpj_raiz") == F.col("emp.cnpj_raiz"), "left")
        .join(simples.alias("s"), F.col("e.cnpj_raiz") == F.col("s.cnpj_raiz"), "left")
        .join(cnae.alias("cnae"), F.col("e.cnae_principal") == F.col("cnae.codigo"), "left")
        .join(natureza.alias("nat"), F.col("emp.natureza_juridica") == F.col("nat.codigo"), "left")
        .join(municipio.alias("m"), F.col("e.municipio") == F.col("m.codigo"), "left")
        .select(
            # Informações da empresa
            F.col("e.cnpj_raiz"),
            F.col("e.documento_sem_formatacao").alias("cnpj_sem_formatacao"),
            F.col("e.documento_formatado").alias("cnpj_formatado"),
            F.when(F.col("e.is_matriz") == True, "Sim").otherwise("Nao").alias("flag_matriz"),
            F.col("emp.razao_social"),
            F.col("e.nome_fantasia"),
            F.col("e.situacao_cadastral"),
            F.to_date(F.col("e.data_situacao_cadastral"), "yyyy-MM-dd").alias("data_situacao_cadastral"),
            F.to_date(F.col("e.data_inicio_atividade"), "yyyy-MM-dd").alias("data_fundacao"),
            F.col("e.cnae_principal").alias("cnae_principal_codigo"),
            F.col("cnae.descricao").alias("cnae_principal_descricao"),
            F.col("e.cnae_secundaria").alias("cnae_secundaria"),
            F.col("e.logradouro"),
            F.col("e.numero"),
            F.col("e.complemento"),
            F.col("e.bairro"),
            F.col("e.cep"),
            F.col("e.uf"),
            F.col("m.descricao").alias("municipio"),
            F.col("e.situacao_especial"), 
            F.to_date(F.col("e.data_sitaucao_especial"), "yyyy-MM-dd").alias("data_situacao_especial"),
            F.col("emp.natureza_juridica").alias("natureza_juridica_codigo"),
            F.col("nat.descricao").alias("descricao_natureza_juridica"),
            F.col("emp.porte_empresa"),
            F.col("emp.capital_social_empresa").cast("double").alias("capital_social_empresa"),
            F.when(F.col("s.is_simples") == True, "Sim").otherwise("Nao").alias("flag_simples"),
            F.to_date(F.col("s.data_opcao_pelo_simples"), "yyyy-MM-dd").alias("data_opcao_pelo_simples"),
            F.to_date(F.col("s.data_exclusao_simples"), "yyyy-MM-dd").alias("data_exclusao_simples"),
            F.when(F.col("s.is_mei") == True, "Sim").otherwise("Nao").alias("flag_mei"),
            F.to_date(F.col("s.data_opcao_pelo_mei"), "yyyy-MM-dd").alias("data_opcao_pelo_mei"),
            F.to_date(F.col("s.data_exclusao_mei"), "yyyy-MM-dd").alias("data_exclusao_mei"),
    
            # Informações dos sócios
            F.col("soci.identificador_socio").alias("tipo_socio"),
            F.col("soci.documento_socio").alias("documento_socio_mais_recente"),
            F.col("soci.nome_razao_social").alias("socio_nome"),
            F.col("soci.qualificacao_socio").alias("socio_qualificacao_codigo"),
            F.col("q.descricao").alias("socio_qualificacao_descricao"),
            F.to_date(F.col("soci.data_entrada_sociedade"), "yyyy-MM-dd").alias("data_entrada_sociedade"),
    
            # Informações PEP
            F.col("pep_info.pep_nome"),
            F.col("pep_info.pep_funcao"),
            F.col("pep_info.pep_nome_orgao"),
            F.col("pep_info.pep_documento"),
    
            # Outras informações
            F.col("e.data_ref"),
            F.date_format(F.current_timestamp(), "yyyy-MM-dd").alias("Atualizado_em")
        )
    )

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
