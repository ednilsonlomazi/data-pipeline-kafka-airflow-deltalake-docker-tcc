FROM apache/airflow:2.7.1-python3.10

USER root

# Instala o Java 17 explicitamente
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk ant && \
    apt-get clean;

# Define a variável de ambiente para o Spark saber onde está o Java 17
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

# Instala as bibliotecas do Python
RUN pip install pyspark==3.5.0 delta-spark==3.0.0 pendulum

# -------------------------- Baixando Drivers do S3 - Necessários para comunicação com MinIO ---------------------
USER root

# Definir as versões compatíveis com Spark 3.4/3.5
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262

# -- para encontrar o local onde inserir os JARs, rode:
#    sudo docker exec -it ct-airfloow-scheduler bash
#    $ pip show pyspark
# -- atualizar variavel abaixo com o local
ENV SPARK_JARS_DIR=/home/airflow/.local/lib/python3.10/site-packages/pyspark/jars

# Baixar os JARs diretamente para a pasta de dependências do Spark
RUN curl -sS https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    -o ${SPARK_JARS_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    && curl -sS https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    -o ${SPARK_JARS_DIR}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Voltar para o usuário airflow para manter a segurança
USER airflow

# Versão do Delta compatível com Spark 3.4/3.5
ENV DELTA_VERSION=3.0.0

# Baixar o JAR do Delta Core
RUN curl -sS https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar \
    -o ${SPARK_JARS_DIR}/delta-spark_2.12-${DELTA_VERSION}.jar \
    && curl -sS https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar \
    -o ${SPARK_JARS_DIR}/delta-storage-${DELTA_VERSION}.jar