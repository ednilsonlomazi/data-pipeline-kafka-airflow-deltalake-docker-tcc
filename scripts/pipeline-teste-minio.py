from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Inicializa a sessão com as configurações que discutimos
spark = SparkSession.builder \
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

# 1. Cria um dado simples
data = [("Teste Conexao", 2024), ("MinIO OK", 2025)]
schema = StructType([
    StructField("status", StringType(), True),
    StructField("ano", IntegerType(), True)
])

df = spark.createDataFrame(data, schema)

# 2. Tenta salvar no bucket 'raw' que o seu init-container criou
try:
    print("Tentando gravar no MinIO...")
    df.write.format("delta").mode("overwrite").save("s3a://raw/teste_infra")
    print("Sucesso! O Spark gravou os dados no MinIO.")
    
    # 3. Tenta ler de volta para garantir que a leitura também funciona
    df_read = spark.read.format("delta").load("s3a://raw/teste_infra")
    df_read.show()
    print("Leitura concluída com sucesso!")
except Exception as e:
    print(f"Erro na integração: {e}")

spark.stop()