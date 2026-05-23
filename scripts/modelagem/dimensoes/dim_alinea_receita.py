from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import sys
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp, lit

# 1. Configuração dos Caminhos (Lendo do arquivo local e salvando na pasta 'refined' do MinIO)
path_csv = "/opt/airflow/data/financas/dm_alinea_rec.csv"
path_refined = "s3a://refined/dim_alinea_receita"

arg = sys.argv[1] if len(sys.argv) > 1 else 'run'

# Inicialização da Sessão Spark com suporte ao Delta Lake e MinIO

spark = SparkSession.builder \
    .appName("AppDimAlineaReceita") \
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

# Definição do Schema baseado nas colunas enviadas
schema_csv = StructType([
    StructField("id_alinea", IntegerType(), True),
    StructField("cd_alinea", StringType(), True),  # String previne perda de formatação se houver zeros à esquerda
    StructField("nome", StringType(), True)
])

# --- MODO SETUP: Inicializa a tabela Delta vazia na camada Refined se ela não existir ---
if arg == 'setup':
    if not DeltaTable.isDeltaTable(spark, path_refined):
        # DataFrame vazio com o schema + metadados de auditoria
        df_vazio = spark.createDataFrame([], schema_csv) \
            .withColumn("dt_alteracao", current_timestamp())
            
        df_vazio.write \
            .format("delta") \
            .save(path_refined)
        print("Tabela Delta 'dim_alinea_receita' inicializada com sucesso na camada Refined.")
    else:
        print("A tabela já existe na camada Refined. Setup ignorado.")
    
    spark.stop()
    sys.exit(0)

else:
    # --- MODO RUN (EXECUÇÃO DO PIPELINE) ---
    try:
        print(f"1) Lendo dados do arquivo CSV: {path_csv}")
        # Lendo o CSV mapeando o caractere separador ';' (padrão dos seus outros arquivos)
        df_csv = spark.read \
            .option("header", "true") \
            .option("delimiter", ";") \
            .schema(schema_csv) \
            .csv(path_csv)

        # Adiciona uma coluna de timestamp para saber quando a dimensão foi atualizada/inserida
        df_updates = df_csv.withColumn("dt_alteracao", current_timestamp())

        print(f"2) Iniciando o processo de MERGE na pasta: {path_refined}")
        dt_refined = DeltaTable.forPath(spark, path_refined)

        # 3) Configuração do MERGE usando a chave primária 'id_alinea'
        dt_refined.alias("dim").merge(
            df_updates.alias("upd"),
            "dim.id_alinea = upd.id_alinea"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

        print("Processamento de atualização da dim_alinea_receita concluído com sucesso!")

    except Exception as e:
        print("Erro no processamento da camada Refined")
        raise e
    finally:
        spark.stop()