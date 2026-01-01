from pyspark.sql import SparkSession


class SparkRawIngestion:
    def __init__(self, bootstrap_servers='kafka:29092'):
        self.bootstrap_servers = bootstrap_servers
        # A SparkSession já configurada com os JARs instalados no Docker

# 1. Definição das versões exatas para Spark 3.5 e Delta 3.0
        KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        DELTA_PKG = "io.delta:delta-spark_2.12:3.0.0"
        HADOOP_AWS_PKG = "org.apache.hadoop:hadoop-aws:3.3.4"
        AWS_SDK_PKG = "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        
        PACKAGES = f"{KAFKA_PKG},{DELTA_PKG},{HADOOP_AWS_PKG},{AWS_SDK_PKG}"

        self.spark = SparkSession.builder \
            .appName("KafkaToRaw-Streaming") \
            .config("spark.jars.packages", PACKAGES) \
            .config("spark.jars.ivy", "/app/.ivy2") \
            .config("spark.hadoop.user.name", "admin") \
            .config("spark.driver.extraJavaOptions", "-Duser.home=/app -Djava.io.tmpdir=/tmp") \
            .config("spark.executor.extraJavaOptions", "-Duser.home=/app -Djava.io.tmpdir=/tmp") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://ct-minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "admin") \
            .config("spark.hadoop.fs.s3a.secret.key", "password123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.delta.logStore.s3a.impl", "io.delta.storage.S3SingleDriverLogStore") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("INFO")

        try:
            print("Testando conexão com MinIO...")
            self.spark.range(1).write.format("noop").mode("overwrite").save("s3a://raw/test_connection")
            print("Conexão OK!")
        except Exception as e:
            print(f"Erro crítico de conexão com MinIO: {e}")


    def process_topic_to_raw(self, topic_name, wait=True):
        print(f"Iniciando ingestão streaming do tópico: {topic_name}")

        # 1. Leitura do Kafka em tempo real
        # O Spark trata o tópico como uma tabela que não para de crescer
        df_kafka = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()

        # 2. Transformação básica (O Kafka envia os dados em binário na coluna 'value')
        # Converte o binário para String (JSON)
        df_raw = df_kafka.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp")

        # 3. Escrita no MinIO em formato Delta
        # O 'checkpointLocation' guarda o estado do consumo no MinIO
        query = df_raw.writeStream \
            .format("delta") \
            .queryName(topic_name) \
            .outputMode("append") \
            .option("checkpointLocation", f"s3a://raw/checkpoints/{topic_name}") \
            .trigger(processingTime='1 minute') \
            .start(f"s3a://raw/{topic_name}")
        
        if wait:
            query.awaitTermination()

        # Mantém o processo vivo escutando o Kafka
        return query