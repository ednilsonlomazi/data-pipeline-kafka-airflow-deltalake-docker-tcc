from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys


# Caminhos
path_trusted = "s3a://trusted/tab_trafego"
path_dim_praca = "s3a://refined/dim_praca"
path_dim_veiculo = "s3a://refined/dim_tipo_veiculo"
path_dim_pagamento = "s3a://refined/dim_tipo_pagamento"
path_fato = "s3a://refined/fato_volume_trafego"


spark = SparkSession.builder \
    .appName("Merge-Raw-to-Trusted") \
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

    # carrega tab_trafego
    df_trafego = spark.read.format("delta").load(path_trusted)

    df_trafego_agg = df_trafego.groupBy(
        "desc_praca", 
        "desc_tipo_veiculo", 
        "desc_tipo_pagamento",
        "data"
    ).agg(
        F.count("*").alias("qtd_veiculos"),
        F.current_timestamp().alias("timestamp")
    )

    # carrega dimensoes para psterior join
    dim_praca = spark.read.format("delta").load(path_dim_praca).select("sk_dim_praca", "desc_praca")
    dim_veiculo = spark.read.format("delta").load(path_dim_veiculo).select("sk_dim_tipo_veiculo", "desc_tipo_veiculo")
    dim_pagamento = spark.read.format("delta").load(path_dim_pagamento).select("sk_dim_tipo_pagamento", "desc_tipo_pagamento")

    # 3. Join para buscar as SKs (Substituindo os nomes pelos IDs)
    df_fato = df_trafego_agg \
        .join(dim_praca, "desc_praca", "left") \
        .join(dim_veiculo, "desc_tipo_veiculo", "left") \
        .join(dim_pagamento, "desc_tipo_pagamento", "left")
    



    if not DeltaTable.isDeltaTable(spark, path_fato):

        df_fato.write.format("delta").mode("overwrite").save(path_fato)
    
        print("Fato de tráfego gerada com sucesso!")

    else:
        
        dt_fato = DeltaTable.forPath(spark, path_fato)
        

        dt_fato.alias("target").merge(
            df_fato.alias("source"),
            "target.desc_tipo_pagamento = source.desc_tipo_pagamento AND " \
            "target.desc_tipo_veiculo = source.desc_tipo_veiculo AND " \
            "target.desc_praca = source.desc_praca AND " \
            "target.data = source.data"
        ).whenMatchedUpdate(set={
            # colunas para update: valor novo 
            "timestamp": "current_timestamp()",
            "qtd_veiculos": "source.qtd_veiculos"
        }).whenNotMatchedInsert(values={
            "sk_dim_tipo_pagamento": "source.sk_dim_tipo_pagamento",
            "sk_dim_tipo_veiculo": "source.sk_dim_tipo_veiculo",
            "sk_dim_praca": "source.sk_dim_praca",
            "desc_tipo_pagamento": "source.desc_tipo_pagamento",
            "desc_tipo_veiculo": "source.desc_tipo_veiculo",
            "desc_praca": "source.desc_praca",
            "data": "source.data",
            "qtd_veiculos": "source.qtd_veiculos",
            "timestamp": "current_timestamp()"
        }).execute()

except Exception as e:
    print(f"Erro na geração da Fato: {e}")
    raise e
finally:
    spark.stop()