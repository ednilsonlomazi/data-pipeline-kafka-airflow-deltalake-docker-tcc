from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# Inicializa a sessão (mantendo suas configs de S3/MinIO)
spark = SparkSession.builder \
    .appName("Merge-Raw-to-Trusted") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://ct-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()


path_trusted = "s3a://trusted/tab_trafego"

topicos = ('l01', 'l03', 'l06')
for topico in topicos:

    path_raw = f"s3a://raw/{topico}"
    print(f"--------- Iniciando processamento no topico: {topico} --------------")
    try:
        # 1. Lê os novos dados da RAW
        df_novos_dados = spark.read.format("delta").load(path_raw)
        
        # 2. Verifica se a tabela Trusted já existe para fazer o Merge
        if not DeltaTable.isDeltaTable(spark, path_trusted):
            print(f"Caminho {path_trusted} não encontrado... criando-o pela primeira vez.")
            df_novos_dados.write.format("delta").mode("overwrite").save(path_trusted)
        else:
            dt_trusted = DeltaTable.forPath(spark, path_trusted)

            print("Merge iniciado")
            dt_trusted.alias("trusted").merge(
                df_novos_dados.alias("updates"),
                "trusted.id_msn = updates.id_msn"
            ).whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
            
        print("Merge concluído com sucesso!")

    except Exception as e:
        print(f"Erro no Merge: {e}")
 
spark.stop()