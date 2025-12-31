from pyspark.sql import SparkSession


class SparkRawIngestion:
    def __init__(self, bootstrap_servers='kafka:29092'):
        self.bootstrap_servers = bootstrap_servers
        # A SparkSession já configurada com os JARs instalados no Docker
        self.spark = SparkSession.builder \
            .appName("KafkaToRaw-Streaming") \
            .config("spark.driver.extraJavaOptions", "-Duser.home=/app -Djava.io.tmpdir=/tmp") \
            .config("spark.executor.extraJavaOptions", "-Duser.home=/app -Djava.io.tmpdir=/tmp") \
            .config("spark.jars.ivy", "/app/.ivy2") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio-storage:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "admin") \
            .config("spark.hadoop.fs.s3a.secret.key", "password123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()

    def process_topic_to_raw(self, topic_name, wait=True):
        print(f"Iniciando ingestão streaming do tópico: {topic_name}")

        # 1. Leitura do Kafka em tempo real
        # O Spark trata o tópico como uma tabela que não para de crescer
        df_kafka = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "earliest") \
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
            .start(f"s3a://raw/{topic_name}")
        
        if wait:
            query.awaitTermination()

        # Mantém o processo vivo escutando o Kafka
        return query