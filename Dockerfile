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