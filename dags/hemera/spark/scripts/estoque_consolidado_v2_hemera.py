# Importando Libs
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import lit, coalesce, col, last_day, when, to_date, concat, date_format,year,month, expr, sum, min, max, unix_timestamp, lpad, last, regexp_replace
from pyspark.sql.types import DoubleType, StringType
import os
import re
from datetime import datetime, timezone, timedelta

def estoque_consolidado(spark):

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
    caminho = "estoque_diaria"
    anos = ['2021','2022','2023','2024','2025']
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

    if df_list:
        df = df_list[0]  
        for df_atual in df_list[1:]:
            df = df.unionByName(df_atual, allowMissingColumns=True)
    else:
        print("Nenhum arquivo encontrado. Encerrando a função.")
        return None

    print("Arquivos Lidos")

    df_consolidado = df
    print("Dataframe Inicial")
    df_consolidado.show(5)

    # Criar coluna 'data_fechamento' como o último dia do mês
    df_consolidado = df_consolidado.withColumn("data_arquivo", to_date(lit(data_arquivo), "yyyy-MM-dd"))
    df_consolidado = df_consolidado.withColumn("data_fechamento", last_day(col("data_arquivo")))



    colunas_desejadas = ['tipo_cedente', 'cnpj_cedente', 'nome_cedente',
                      'tipo_sacado', 'cnpj_sacado', 'nome_sacado',
                     'id_titulo', 'tipo_titulo', 'data_emissao', 'data_aquisicao',
                     'data_vencimento', 'numero_boleto_banco', 'numero_titulo', 'campo_chave',
                      'valor_aquisicao', 'valor_nominal', 'valor_presente', 'pdd_nota', 
                      'pdd_vencido', 'data_arquivo',
                     'data_prorrogacao', 'data_ocorrencia_prorrogacao', 'coobrigacao',
                     'cnpj_originador','data_ref']

    # Filtrando as colunas desejadas
    df_consolidado = df_consolidado.select(*colunas_desejadas)

    # Garantir que numero_titulo e id_titulo contenham apenas números e tenham exatamente 10 caracteres
    df_consolidado = df_consolidado.withColumn("numero_titulo", lpad(regexp_replace(col("numero_titulo"), r"[^\d]", ""), 10, "0"))
    df_consolidado = df_consolidado.withColumn("id_titulo", lpad(col("id_titulo").cast(StringType()), 10, "0"))

    # Remover espaços em branco nas colunas de texto
    df_consolidado = df_consolidado.withColumn("nome_cedente", 
                                               regexp_replace(col("nome_cedente"), r'^\s+|\s+$', ''))
    df_consolidado = df_consolidado.withColumn("nome_sacado", 
                                               regexp_replace(col("nome_sacado"), r'^\s+|\s+$', ''))

    # Conversão das colunas para datetime
    df_consolidado = df_consolidado.withColumn("data_arquivo", 
                                               unix_timestamp("data_arquivo", "yyyy-MM-dd").cast("timestamp"))
    df_consolidado = df_consolidado.withColumn("data_aquisicao", 
                                               unix_timestamp("data_aquisicao", "yyyy-MM-dd").cast("timestamp"))
    df_consolidado = df_consolidado.withColumn("data_vencimento", 
                                               unix_timestamp("data_vencimento", "yyyy-MM-dd").cast("timestamp"))

    # Calculando pddtotal
    df_consolidado = df_consolidado.withColumn("pddtotal",coalesce(col("pdd_nota"), lit(0)) + coalesce(col("pdd_vencido"), lit(0)))

    # Agrupando os dados
    df_agrupado = df_consolidado.groupBy('data_ref', 'id_titulo', 'cnpj_sacado', 'nome_sacado', 
                                        'cnpj_cedente', 'nome_cedente').agg(
        min("data_aquisicao").alias('data_aquisicao'),
        min("data_vencimento").alias('data_vencimento'),
        min("valor_nominal").alias('valor_nominal'),
        min("valor_aquisicao").alias('valor_aquisicao'),
        min("numero_titulo").alias('numero_titulo')
    )

    # Ordenando os dados
    df_consolidado = df_consolidado.orderBy("data_arquivo")

    # Agrupar por IdTituloVx
    df_novos_valores = df_consolidado.groupBy("data_ref", "id_titulo").agg(
        F.expr("first(valor_presente, true)").alias("estoque_valor_presente_inicial"),
        F.expr("last(valor_presente, true)").alias("estoque_valor_presente_final"),
        F.expr("first(data_arquivo, true)").alias("primeira_data"),
        F.expr("last(data_arquivo, true)").alias("ultima_data"),
        F.expr("first(pddtotal, true)").alias("pdd_inicial"),
        F.expr("last(pddtotal, true)").alias("pdd_final")
    )

    # Juntando com o DataFrame original
    df_final = df_agrupado.join(df_novos_valores, on=['id_titulo', 'data_ref'], how='left')


    print('df_final pdd_inicial e pdd_final')
    df_final.show(10)

    # Calculando o estoque de valores com base nas datas
    data_abertura = df_consolidado.agg(min("data_arquivo")).collect()[0][0]
    data_fechamento = df_consolidado.agg(max("data_arquivo")).collect()[0][0]

    def formatar_valores_com_base_em_datas(df, coluna_valor, coluna_data, data_referencia, valor_padrao=0):
        return df.withColumn(
            coluna_valor,
            when(col(coluna_data) != lit(data_referencia), valor_padrao).otherwise(col(coluna_valor))
        )

    # Aplicando a função nas colunas necessárias
    df_final = formatar_valores_com_base_em_datas(df_final, 'estoque_valor_presente_inicial', 'primeira_data', data_abertura)
    df_final = formatar_valores_com_base_em_datas(df_final, 'estoque_valor_presente_final', 'ultima_data', data_fechamento)
    df_final = formatar_valores_com_base_em_datas(df_final, 'pdd_inicial', 'primeira_data', data_fechamento)
    df_final = formatar_valores_com_base_em_datas(df_final, 'pdd_final', 'ultima_data', data_fechamento)


    # Renomeando e organizando as colunas
    df_final = df_final.withColumnRenamed("ultima_data", "data_fechamento")


    # Reorganizando as colunas
    colunas = ['data_fechamento'] + [col for col in df_final.columns if col != 'data_fechamento']
    df_final = df_final.select(*colunas)

    # Convertendo colunas para string
    colunas_para_converter_datetime = ['data_aquisicao', 'data_vencimento']
    df_final = df_final.withColumn('data_aquisicao', date_format('data_aquisicao', 'yyyy-MM-dd'))
    df_final = df_final.withColumn('data_vencimento', date_format('data_vencimento', 'yyyy-MM-dd'))

    df_final = df_final.withColumn(
    "data_fechamento",
    date_format(last_day(to_date(concat(col("data_ref"), lit("-01")), "yyyy-MM-dd")), "yyyy-MM-dd")
    )

    # Adicionar colunas 'year' e 'month' extraídas de 'data_fechamento'
    df_final = df_final.withColumn("year", year(col("data_fechamento")).cast(StringType()))
    df_final = df_final.withColumn("month", month(col("data_fechamento")).cast(StringType()))

    # Adicionando data de atualização
    now = datetime.now(tz=timezone(timedelta(hours=-3)))
    df_final = df_final.withColumn("atualizado_em", lit(now.strftime('%Y-%m-%d %H:%M:%S')))


    # Finalizando
    print("DataFrame Final")
    df_final.show()

    
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
        .option("overwriteSchema", "true") \
        .mode("overwrite") \
        .save("s3a://hemera-consolidado/estoque_consolidado")
    print("Arquivos Salvos")

    spark.stop()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("estoque_consolidado") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    estoque_consolidado(spark) 