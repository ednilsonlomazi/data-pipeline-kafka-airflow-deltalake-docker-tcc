from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import sys
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import regexp_replace

path_trusted = "s3a://trusted/tab_receita"
arg = sys.argv[1]
path_raw = f"s3a://raw/l01"


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


# schema que vai receber o parse do json vindo da raw
schema_json = StructType([
    StructField("id_tempo", StringType(), True),             
    StructField("id_unidade_orc", StringType(), True),       
    StructField("id_origem", StringType(), True),           
    StructField("id_especie", StringType(), True),          
    StructField("id_rubrica", StringType(), True),          
    StructField("id_alinea", StringType(), True),           
    StructField("id_subalinea", StringType(), True),        
    StructField("id_item", StringType(), True),             
    StructField("id_fonte", StringType(), True),            
    StructField("cd_fonte_recurso", StringType(), True),     
    StructField("cd_classificacao_rec", StringType(), True),  
    StructField("ano_particao", StringType(), True),         
    StructField("vr_efetivado", StringType(), True), #String          
    
    # Metadados de controle e auditoria injetados pelo pipeline do Lakehouse
    StructField("id_msn", StringType(), True),                
    StructField("timestamp_ingestao", StringType(), True),     
    StructField("datastamp", StringType(), True)              
])


# schema de persistencia no datalake
schema_trusted = StructType([
    StructField("id_tempo", StringType(), True),             
    StructField("id_unidade_orc", StringType(), True),       
    StructField("id_origem", StringType(), True),           
    StructField("id_especie", StringType(), True),          
    StructField("id_rubrica", StringType(), True),          
    StructField("id_alinea", StringType(), True),           
    StructField("id_subalinea", StringType(), True),        
    StructField("id_item", StringType(), True),             
    StructField("id_fonte", StringType(), True),            
    StructField("cd_fonte_recurso", StringType(), True),     
    StructField("cd_classificacao_rec", StringType(), True),  
    StructField("ano_particao", StringType(), True),         
    StructField("vr_efetivado", DoubleType(), True), #Double         
    
    # Metadados de controle e auditoria injetados pelo pipeline do Lakehouse
    StructField("id_msn", StringType(), True),                
    StructField("timestamp_ingestao", StringType(), True),     
    StructField("datastamp", StringType(), True)              
])

# Função que o Spark vai chamar para cada "pedaço" (micro-batch) novo de dados, <Near-realtime>
def upsert_to_delta(microBatchDf, batchId):
    print("Merge Iniciado...")

    df_tab = spark.read.format("delta").load(path_trusted)
    
    print(f"Batch ID: {batchId}\nQtd. registros do batch: {microBatchDf.count()}\nQtd. Registros em {path_trusted}: {df_tab.count()}")
    microBatchDf.show(20, False)

    # Instanciar para gerenciar/modificar
    dt_trusted = DeltaTable.forPath(spark, path_trusted)
    
    dt_trusted.alias("trusted").merge(

        microBatchDf.alias("updates"),
        "trusted.id_msn = updates.id_msn"

    ).whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()


    df_final = spark.read.format("delta").load(path_trusted)
    print(f"Merge Finalizado...\nQtd. Registros em {path_trusted}: {df_tab.count()}")


if arg == 'setup':
    print("Setup Iniciado...")

    if not DeltaTable.isDeltaTable(spark, path_trusted):

        # Cria um DataFrame vazio com o schema definido
        df_vazio = spark.createDataFrame([], schema_trusted)
        df_vazio.write.partitionBy("datastamp").format("delta").save(path_trusted)
        
        print(f"Tabela {path_trusted} criada com suscesso!")

    else:
        print(f"Tabela {path_trusted} já existe no Minio")

    spark.stop()
    sys.exit(0)


try:
    print(f"Iniciando Processamento Incremental de {path_trusted}")

    # Instanciando o canal de Streamming...
    df_raw_stream = spark.readStream.format("delta").load(path_raw)
  

    print("Realizando o parse do JSON baseado no schema_json...")
    df_novos_dados = df_raw_stream.select(

        from_json(col("value").cast("string"), schema_json).alias("data")

    ).select("data.*")


    df_novos_dados = df_novos_dados.withColumn(
        "vr_efetivado",
        regexp_replace("vr_efetivado", ",", ".")
    )    

    print("Iniciando padronizacao de TIpos de dados")
    df_novos_dados = df_novos_dados.select(
        col("id_tempo").cast(StringType()),
        col("id_unidade_orc").cast(StringType()),
        col("id_origem").cast(StringType()),
        col("id_especie").cast(StringType()),
        col("id_rubrica").cast(StringType()),
        col("id_alinea").cast(StringType()),
        col("id_subalinea").cast(StringType()),
        col("id_item").cast(StringType()),
        col("id_fonte").cast(StringType()),
        col("cd_fonte_recurso").cast(StringType()),
        col("cd_classificacao_rec").cast(StringType()),
        col("ano_particao").cast(StringType()),
        col("vr_efetivado").cast(DoubleType()).alias("vr_efetivado"), #double
        col("id_msn").cast(StringType()),
        col("timestamp_ingestao").cast(StringType()),
        col("datastamp").cast(StringType())
    )    
    
   

    print(f"Iniciando escrita incremental de origem: {path_raw}")

    # Trigger: AvailableNow: Isso faz o Spark processar apenas o que há de novo e PARAR (como uma DAG comum)
    query = df_novos_dados.writeStream \
        .foreachBatch(upsert_to_delta) \
        .option("checkpointLocation", f"s3a://trusted/checkpoints/l01") \
        .trigger(availableNow=True) \
        .start()
   
    query.awaitTermination()

    print("Processamento incremental concluído!")
    


except Exception as e:
    print("Erro no processamento")
    raise e

finally:
    spark.stop()