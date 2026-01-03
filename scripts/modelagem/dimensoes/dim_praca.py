from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys


path_dim = "s3a://refined/dim_praca"


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
    # gera um dataframe com desc_praca distintos
    df_source = spark.read.format("delta").load("s3a://trusted/tab_trafego") \
        .select("desc_praca").distinct()

    if not DeltaTable.isDeltaTable(spark, path_dim):
        
        # apenas uma variavel que é parte de uma "Window Funcion em paralelo ao SQL"
        windowSpec = Window.orderBy("desc_praca")

        # adiciona a coluna sk_dim_praca, que eh o numero da linha correspondente da tabela ordenada por desc_praca
        # adiciona timestamp para saber o horario do insert
        df_initial = df_source.withColumn("sk_dim_praca", F.row_number().over(windowSpec)) \
                             .withColumn("timestamp", F.current_timestamp()) # Adicionado para manter padrão
        
        df_initial.write.format("delta").save(path_dim)

        print("Tabela dim_praca criada com Schema e Constraints com sucesso!")

    else:
        # instancia a DeltaTable da dimensao existente
        dt_target = DeltaTable.forPath(spark, path_dim)
        
        # faz um left_anti que em SQL seria um join com where not exists... retorna tudo do df_source que nao existe no target
        df_new_records = df_source.join(dt_target.toDF(), "desc_praca", "left_anti")
        
        # se existir dado novo, insere
        if df_new_records.count() > 0:

            # pega o maior sk_dim da tabela 
            max_id = dt_target.toDF().agg(F.max("sk_dim_praca")).collect()[0][0]

            # cria a sk_dim adicionando a max_id, entao se o sk_dim maior eh 10, entao 10 + 1 (row_number) = 11 e por ai vai...
            windowSpec = Window.orderBy("desc_praca")
            df_new_records = df_new_records.withColumn("sk_dim_praca", F.row_number().over(windowSpec) + max_id)
            

            # df_source lef join df_new_records <<like sql>>
            # esse df fica com sk_dim null quando NAO eh novo, e os novos fica com sk_dim preenchido
            df_final_source = df_source.join(
                df_new_records.select("desc_praca", "sk_dim_praca"), 
                "desc_praca", 
                "left"
            )
        else:
            df_final_source = df_source.withColumn("sk_dim_praca", F.lit(None).cast("int"))

        dt_target.alias("target").merge(
            df_final_source.alias("source"),
            "target.desc_praca = source.desc_praca"
        ).whenMatchedUpdate(set={
            # colunas para update: valor novo 
            "timestamp": "current_timestamp()"
        }).whenNotMatchedInsert(values={
            # colunas para insert: valor da coluna
            "sk_dim_praca": "source.sk_dim_praca",
            "desc_praca": "source.desc_praca",
            "timestamp": "current_timestamp()" 
        }).execute()

except Exception as e:
    print("Erro no processamento")
    raise e

finally:
    spark.stop()