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
        "id_alinea", 
        "id_item", 
        "id_origem",
        "id_rubrica",
        "ano_particao"
    ).agg(
        F.sum("vr_efetivado").alias("val_efetivado")
    )

    # carrega dimensoes para psterior join
    dim_alinea_receita = spark.read.format("delta").load(path_dim_alinea_receita).select("sk_dim_alinea_receita", "id_alinea")
    dim_item_receita = spark.read.format("delta").load(path_dim_item_receita).select("sk_dim_item_receita", "id_item")
    dim_origem_receita = spark.read.format("delta").load(path_dim_origem_receita).select("sk_dim_origem_receita", "id_origem")
    dim_rubrica_receita = spark.read.format("delta").load(path_dim_rubrica_receita).select("sk_dim_rubrica_receita", "id_rubrica")    

# 3. Join para buscar as SKs especificando lado esquerdo e direito
    df_fato_receita = df_tab_receita_agg.alias("fato") \
    .join(
        dim_alinea_receita.alias("dim_alinea"), 
        F.col("dim_alinea.id_alinea") == F.col("fato.id_alinea"), 
        "left"
    ) \
    .join(
        dim_item_receita.alias("dim_item"), 
        F.col("dim_item.id_item") == F.col("fato.id_item"), 
        "left"
    ) \
    .join(
        dim_origem_receita.alias("dim_origem"), 
        F.col("dim_origem.id_origem") == F.col("fato.id_origem"), 
        "left"
    ) \
    .join(
        dim_rubrica_receita.alias("dim_rubrica"), 
        F.col("dim_rubrica.id_rubrica") == F.col("fato.id_rubrica"),
        "left" # Mantendo o seu padrão (sem passar "left", o Spark assume inner)
    )\
    .select(
        # 4. Seleção explícita utilizando os aliases criados nos joins
        F.col("dim_alinea.sk_dim_alinea_receita"),
        F.col("dim_item.sk_dim_item_receita"),
        F.col("dim_origem.sk_dim_origem_receita"),
        F.col("dim_rubrica.sk_dim_rubrica_receita"),
        F.col("fato.id_alinea"),
        F.col("fato.id_item"),
        F.col("fato.id_origem"),
        F.col("fato.id_rubrica"),
        F.col("fato.ano_particao"),
        F.col("fato.val_efetivado")
    )



    if not DeltaTable.isDeltaTable(spark, path_fato):

        df_fato_receita.write.format("delta").mode("overwrite").save(path_fato)
    
        print("Fato Receita gerada com sucesso!")

    else:
        
        df_fato = DeltaTable.forPath(spark, path_fato)
        

        df_fato.alias("target").merge(
            df_fato_receita.alias("source"),
            "target.id_alinea = source.id_alinea AND " \
            "target.id_item = source.id_item AND " \
            "target.id_origem = source.id_origem AND " \
            "target.id_rubrica = source.id_rubrica AND " \
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