from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import sys
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col

path_trusted = "s3a://trusted/tab_receita"
arg = sys.argv[1]
path_raw = f"s3a://raw/l03"


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

schema_trusted = StructType([
    StructField("id_tempo", IntegerType(), True),           # ID de tempo / Calendário
    StructField("id_unidade_orc", StringType(), True),      # Código da Unidade Orçamentária
    StructField("id_origem", StringType(), True),           # Código de origem da receita
    StructField("id_especie", StringType(), True),          # Código de espécie da receita
    StructField("id_rubrica", StringType(), True),          # Código de rúbrica
    StructField("id_alinea", StringType(), True),           # Código de alínea
    StructField("id_subalinea", StringType(), True),        # Código de subalínea
    StructField("id_item", StringType(), True),             # ID do item específico
    StructField("id_fonte", StringType(), True),            # ID da fonte de recursos
    StructField("cd_fonte_recurso", StringType(), True),    # Código estruturado da fonte
    StructField("cd_classificacao_rec", StringType(), True), # Classificação econômica completa (Ex: 1311000000)
    StructField("ano_particao", IntegerType(), True),       # Ano de referência original (Ex: 2002)
    StructField("vr_efetivado", DoubleType(), True),        # Valor real arrecadado (Ex: 155.22)
    
    # Metadados de controle injetados pelo Producer para o Data Lakehouse
    StructField("id_msn", StringType(), False), 
    StructField("timestamp_ingestao", StringType(), True),
    StructField("datastamp", StringType(), True)
])

# Função que o Spark vai chamar para cada "pedaço" (micro-batch) novo de dados, <Near-realtime>
def upsert_to_delta(microBatchDf, batchId):
    print(f"Processando batch ID: {batchId}")
    dt_trusted = DeltaTable.forPath(spark, path_trusted)
    
    dt_trusted.alias("trusted").merge(
        microBatchDf.alias("updates"),
        "trusted.id_msn = updates.id_msn"
    ).whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()


if arg == 'setup':
    if not DeltaTable.isDeltaTable(spark, path_trusted):
        # Cria um DataFrame vazio com o schema definido
        df_vazio = spark.createDataFrame([], schema_trusted)
        df_vazio.write \
            .partitionBy("datastamp") \
            .format("delta").save(path_trusted)
        
        print("Tabela Delta inicializada com sucesso.")
    else:
        print("A pasta já existe no MinIO. Setup ignorado.")

    
    spark.stop()
    sys.exit(0)


try:
    
    df_raw_stream = spark.readStream.format("delta").load(path_raw)

    # faz o parse do json
    df_novos_dados = df_raw_stream.select(
        from_json(col("value").cast("string"), schema_trusted).alias("data")
    ).select("data.*")

    print(f"Iniciando escrita incremental para: l03")

    # Trigger: AvailableNow: Isso faz o Spark processar apenas o que há de novo e PARAR (como uma DAG comum)
    query = df_novos_dados.writeStream \
        .foreachBatch(upsert_to_delta) \
        .option("checkpointLocation", f"s3a://trusted/checkpoints/l03") \
        .trigger(availableNow=True) \
        .start()
    
    query.awaitTermination()
    print("Processamento incremental concluído!")

except Exception as e:
    print("Erro no processamento")
    raise e

finally:
    spark.stop()