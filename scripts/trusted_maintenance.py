from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import sys

arg = sys.argv[1]
path_trusted = f"s3a://trusted/{arg}"

# Configurações de sobrevivência (iguais às suas outras DAGs)
spark = SparkSession.builder \
    .appName("Delta-Maintenance-Trusted") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://ct-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()


try:
    print(f"Iniciando manutenção da tabela: {path_trusted}")
    dt_trusted = DeltaTable.forPath(spark, path_trusted)

    # OPTIMIZE: Agrupa arquivos pequenos e aplica Z-ORDER no ID do MSN
    # Isso acelera drasticamente os próximos MERGEs
    print("Executando OPTIMIZE com Z-ORDER BY id_msn...")
    dt_trusted.optimize().executeZOrderBy("id_msn")

    print("Manutenção concluída com sucesso!")

except Exception as e:
    print(f"Erro na manutenção: {e}")
    raise e
finally:
    spark.stop()