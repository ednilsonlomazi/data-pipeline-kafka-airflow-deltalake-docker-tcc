from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys


# Caminhos
path_tab_receita = "s3a://trusted/tab_receita"

path_dim_alinea_receita = "s3a://refined/dim_alinea_receita"
path_dim_item_receita = "s3a://refined/dim_item_receita"
path_dim_origem_receita = "s3a://refined/dim_origem_receita"
path_dim_rubrica_receita = "s3a://refined/dim_rubrica_receita"

path_fato = "s3a://refined/fato_receita"


spark = SparkSession.builder \
    .appName("GeraFatoReceita") \
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
    df_tab_receita = spark.read.format("delta").load(path_tab_receita)

    df_tab_receita_agg = df_tab_receita.groupBy(
        "id_alinea_receita", 
        "id_item_receita", 
        "id_origem_receita",
        "id_rubrica_receita",
        "ano_particao"
    ).agg(
        F.sum("vr_efetivado").alias("val_efetivado")
    )

    # carrega dimensoes para psterior join
    dim_alinea_receita = spark.read.format("delta").load(path_dim_alinea_receita).select("sk_dim_alinea_receita", "id_alinea_receita")
    dim_item_receita = spark.read.format("delta").load(path_dim_item_receita).select("sk_dim_item_receita", "id_item_receita")
    dim_origem_receita = spark.read.format("delta").load(path_dim_origem_receita).select("sk_dim_origem_receita", "id_origem_receita")
    dim_rubrica_receita = spark.read.format("delta").load(path_dim_rubrica_receita).select("sk_dim_rubrica_receita", "id_rubrica_receita")    

    # 3. Join para buscar as SKs 
    df_fato_receita = df_tab_receita_agg \
        .join(dim_alinea_receita, "id_alinea_receita", "left") \
        .join(dim_item_receita, "id_item_receita", "left") \
        .join(dim_origem_receita, "id_origem_receita", "left") \
        .join(dim_rubrica_receita, "id_rubrica_receita")
    



    if not DeltaTable.isDeltaTable(spark, path_fato):

        df_fato_receita.write.format("delta").mode("overwrite").save(path_fato)
    
        print("Fato Receita gerada com sucesso!")

    else:
        
        dt_fato = DeltaTable.forPath(spark, path_fato)
        

        dt_fato.alias("target").merge(
            df_fato.alias("source"),
            "target.id_alinea_receita = source.id_alinea_receita AND " \
            "target.id_item_receita = source.id_item_receita AND " \
            "target.id_origem_receita = source.id_origem_receita AND " \
            "target.id_rubrica_receita = source.id_rubrica_receita AND " \
            "target.ano_particao = source.ano_particao"
        ).whenMatchedUpdate(set={
            # colunas para update: valor novo 
            "val_efetivado": "source.val_efetivado"
        }).whenNotMatchedInsertAll().execute()

except Exception as e:
    print(f"Erro na geração da Fato: {e}")
    raise e
finally:
    spark.stop()