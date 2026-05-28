from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys


# Caminhos
path_tab_despesa = "s3a://trusted/tab_despesas"

path_dim_contrato_divida = "s3a://refined/dim_contrato_divida"
path_dim_tipo_despesa = "s3a://refined/dim_tipo_divida"
path_dim_favorecido = "s3a://refined/dim_favorecido"

path_fato = "s3a://refined/fato_despesa"


spark = SparkSession.builder \
    .appName("GeraFatoDespesa") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://ct-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.databricks.delta.retry.commit.attempts", "10") \
    .config("spark.databricks.delta.retry.commit.delay", "100ms") \
    .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
    .config("spark.databricks.delta.autoCompact.enabled", "true") \
    .getOrCreate()

try:

    # carrega tab_receita
    df_tab_despesa = spark.read.format("delta").load(path_tab_despesa)

    df_tab_despesa_agg = df_tab_despesa.groupBy(
        "id_contrato", 
        "id_favorecido", 
        "id_tipo",
        "ano_particao"
    ).agg(
        F.sum("vr_amortizacao").alias("vr_amortizacao"),
        F.sum("vr_juros").alias("vr_juros")
    )

    # carrega dimensoes para psterior join
    dim_contrato_divida = spark.read.format("delta").load(path_dim_contrato_divida).select("sk_dim_contrato_divida", "id_contrato")
    dim_favorecido = spark.read.format("delta").load(path_dim_favorecido).select("sk_dim_favorecido", "id_favorecido")
    dim_tipo_despesa = spark.read.format("delta").load(path_dim_tipo_despesa).select("sk_dim_tipo_despesa", "id_tipo")
   

# 3. Join para buscar as SKs especificando lado esquerdo e direito
    df_fato_despesa = df_tab_despesa_agg.alias("fato") \
    .join(
        dim_contrato_divida.alias("dcd"), 
        F.col("dcd.id_contrato") == F.col("fato.id_contrato"), 
        "left"
    ) \
    .join(
        dim_favorecido.alias("dfv"), 
        F.col("dfv.id_favorecido") == F.col("fato.id_favorecido"), 
        "left"
    ) \
    .join(
        dim_tipo_despesa.alias("dtp"), 
        F.col("dtp.id_tipo") == F.col("fato.id_tipo"), 
        "left"
    ) \
    .select(
        # 4. Seleção explícita utilizando os aliases criados nos joins
        F.col("dcd.sk_dim_contrato_divida"),
        F.col("dfv.sk_dim_favorecido"),
        F.col("dtp.sk_dim_tipo_despesa"),
        F.col("fato.id_contrato"),
        F.col("fato.id_favorecido"),
        F.col("fato.id_tipo"),
        F.col("fato.ano_particao"),
        F.col("fato.vr_amortizacao"),
        F.col("fato.vr_juros")
    )



    if not DeltaTable.isDeltaTable(spark, path_fato):

        df_fato_despesa.write.format("delta").mode("overwrite").save(path_fato)
    
        print("Fato Despesa gerada com sucesso!")

    else:
        
        df_fato = DeltaTable.forPath(spark, path_fato)
        

        df_fato.alias("target").merge(
            df_fato_despesa.alias("source"),
            "target.sk_dim_contrato_divida = source.sk_dim_contrato_divida AND " \
            "target.sk_dim_favorecido = source.sk_dim_favorecido AND " \
            "target.sk_dim_tipo_despesa = source.sk_dim_tipo_despesa AND " \
            "target.ano_particao = source.ano_particao" # 
        ).whenMatchedUpdate(set={
            # 
            "id_contrato": "source.id_contrato",
            "id_favorecido": "source.id_favorecido",
            "id_tipo": "source.id_tipo",
            "vr_amortizacao": "source.vr_amortizacao",
            "vr_juros": "source.vr_juros"
        }).whenNotMatchedInsertAll().execute()

except Exception as e:
    print(f"Erro na geração da Fato: {e}")
    raise e
finally:
    spark.stop()