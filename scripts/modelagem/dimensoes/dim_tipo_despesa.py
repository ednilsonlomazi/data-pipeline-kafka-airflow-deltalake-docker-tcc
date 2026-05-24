from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import sys
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp, lit, md5, concat, col

# 1. Configuração dos Caminhos (Lendo do arquivo local e salvando na pasta 'refined' do MinIO)
path_csv = "/opt/airflow/data/financas/dm_tipo_divida.csv"
path_refined = "s3a://refined/dim_tipo_divida"

arg = sys.argv[1] if len(sys.argv) > 1 else 'run'

# Inicialização da Sessão Spark com suporte ao Delta Lake e MinIO
spark = SparkSession.builder \
    .appName("AppDimTipoDivida") \
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

# O SCHEMA AGORA JÁ NASCE COMPLETO (Reflete o estado final da tabela Delta)
schema_final = StructType([
    StructField("sk_dim_tipo_despesa", StringType(), True),
    StructField("id_tipo", IntegerType(), True),
    StructField("cd_tipo", StringType(), True),  
    StructField("nome", StringType(), True),
    StructField("dt_alteracao", StringType(), True)
])

# --- MODO SETUP ---
if arg == 'setup':
    if not DeltaTable.isDeltaTable(spark, path_refined):
        # Cria a tabela vazia seguindo rigorosamente o contrato do schema_final
        df_vazio = spark.createDataFrame([], schema_final)
            
        df_vazio.write \
            .format("delta") \
            .save(path_refined)
        print("Tabela Delta 'dim_tipo_divida' inicializada do zero com Schema estruturado!")
    else:
        print("A tabela já existe. Setup ignorado.")
    
    spark.stop()
    sys.exit(0)

# --- MODO RUN ---
else:
    try:
        print(f"1) Lendo dados do arquivo CSV: {path_csv}")
        
        # Schema de leitura correspondente ao arquivo físico original
        schema_leitura_csv = StructType([
            StructField("id_tipo", IntegerType(), True),
            StructField("cd_tipo", StringType(), True),  
            StructField("nome", StringType(), True)
        ])

        df_csv = spark.read \
            .option("header", "true") \
            .option("delimiter", ";") \
            .schema(schema_leitura_csv) \
            .csv(path_csv)

        # Populando a SK com hash MD5 baseada no id_tipo e adicionando data de alteração
        df_updates = df_csv \
            .withColumn("sk_dim_tipo_despesa", md5(concat(col("id_tipo").cast(StringType()), lit("")))) \
            .withColumn("dt_alteracao", current_timestamp()) \
            .select("sk_dim_tipo_despesa", "id_tipo", "cd_tipo", "nome", "dt_alteracao")

        print(f"2) Iniciando o processo de MERGE na pasta: {path_refined}")
        dt_refined = DeltaTable.forPath(spark, path_refined)

        dt_refined.alias("dim").merge(
            df_updates.alias("upd"),
            "dim.id_tipo = upd.id_tipo"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

        print("Processamento concluído com sucesso!")

    except Exception as e:
        print("Erro no processamento da camada Refined")
        raise e
    finally:
        spark.stop()