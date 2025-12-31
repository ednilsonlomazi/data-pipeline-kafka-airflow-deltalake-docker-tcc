from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys
from pyspark.sql.functions import lit

# ---------- necessario para uma criação dinâmica da DAG por tópico -------------------#
TOPIC = sys.argv[1] # lendo o argumento passado na chamada do script
BASE_PATH = "/opt/airflow/datalakehouse"
TABLE_NAME = "tab_trafego"
INPUT_PATH = f"{BASE_PATH}/raw/{TOPIC}/"
CHECKPOINT_PATH = f"{BASE_PATH}/checkpoints/raw_to_trusted/{TOPIC}"
OUTPUT_PATH = f"{BASE_PATH}/trusted/{TABLE_NAME}"



builder = SparkSession.builder \
    .appName("Teste-Minio-Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://ct-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

# Isso baixa as dependências do Delta Lake automaticamente
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# 2. Definir o Schema (O Streaming exige isso) - "Schema on Read"

schema = StructType([
    StructField("data", StringType(), True),
    StructField("hora", StringType(), True),
    StructField("desc_praca", StringType(), True),
    StructField("desc_tipo_veiculo", StringType(), True),
    StructField("qtd_veiculos", StringType(), True) # Lemos como string e tratamos depois
])


# 3. Ler como STREAM (em vez de read.json, usamos readStream)
df_stream = spark.readStream \
    .schema(schema) \
    .json(INPUT_PATH)

# 4. Transformação simples
df_final = df_stream.withColumn("qtd_veiculos", df_stream["qtd_veiculos"].cast("int"))

# coloca uma coluna adicional para saber de qual topico veio a informação
df_final = df_final.withColumn("origem_topico", lit(TOPIC))

# 5. Salvar usando Checkpoint (A mágica acontece aqui)
query = df_final.writeStream \
    .format("delta") \
    .partitionBy("origem_topico") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(once=True) \
    .start(OUTPUT_PATH) # O PySpark cria essa pasta sozinho!

query.awaitTermination()