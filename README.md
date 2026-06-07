# 🚀 Projeto de TCC: Modern Data Lakehouse com Spark, Delta Lake e Kafka

## 📝 Descrição do Projeto
Este projeto implementa uma arquitetura de **Modern Data Lakehouse** para o processamento de fluxos de dados financeiros, sendo sustentado por uma **infraestrutura conteinerizada** via **Docker**, seguindo padrões de **DataOps**. Nesse sentido, ao utilizar o Docker Compose para orquestrar **Múltiplos Microserviços** (Kafka, Airflow e o storage MinIO), o projeto garante uma arquitetura reprodutível, isolada e escalável.


---

## 🏗️ Arquitetura do Sistema
A arquitetura segue o padrão de medalhão (*Medallion Architecture*):

* **Landing/Raw Zone**: Dados brutos consumidos de tópicos Kafka e armazenados em **Delta Tables**.
* **Trusted Zone**: Dados limpos e unificados, com schema definido e armazenados em **Delta Tables**.
* **Refined Zone (Gold)**: Dados modelados em **Star Schema** (Modelo Dimensional) prontos para consumo por ferramentas de BI, tambem em **Delta Tables**.

---

## 🛠️ Tecnologias Utilizadas
* **Orquestração:** Apache Airflow
* **Processamento de Dados:** Apache Spark (PySpark)
* **Armazenamento de Tabela:** Delta Lake (Transações ACID)
* **Storage (S3 Compatible):** MinIO
* **Mensageria:** Apache Kafka & Zookeeper
* **Banco de Dados (Metadata Airflow):** PostgreSQL
* **Infraestrutura:** Docker e Docker Compose
* **Visualização:** Dremio e Framework Streamlit

---

## 📊 Modelagem de Dados (Star Schema)
Abaixo descrevo os objetos por camadas

## Objetos por camadas
### Raw
* `l01`: Mensagens kafka armazenadas em delta tables de streaming de receitas
* `l03`: Mensagens kafka armazenadas em delta tables de streaming de despesas

### Truested
* `tab_despesa`: Mensagens kafka tipadas e formatadas de valores de despesas
* `tab_receita`: Mensagens kafka tipadas e formatadas de valores de despesas

### Refined
* `dim_contrato_divida`: Contrato de Dívidas.
* `dim_favorecido`: Favorecido.
* `dim_tipo_despesa`: Tipo de despesa.
* `dim_alinea_receita`: .
* `dim_item_receita`: Item da receita.
* `dim_origem_receita`: Origem da Receita.
* `dim_rubrica_receita`: Rubrica Receita.
* `fato_despesa`: Valores de despesas
* `fato_receita`: Valores de receitas

```mermaid
graph TD
    %% Subgraph da Infraestrutura de Mensageria (Kafka)
    subgraph Ingestao_Streaming ["Fluxo de Ingestão e Streaming"]
        ct-zookeeper["ct-zookeeper<br/>Porta: 2181"] -->|Gerencia| ct-kafka["ct-kafka<br/>Broker Port: 9092"]
        ct-kafka -->|Healthcheck OK| ct-kafka-init("ct-kafka-init<br/>Cria tópicos: l01, l03, l06")
        
        ct-kafka-producer["ct-kafka-producer<br/>Producer Service"] -->|Dispara Payloads| ct-kafka
        ct-kafka -->|Consome Mensagens| ct-kafka-consumer["ct-kafka-consumer<br/>Consumer Service"]
    end

    %% Subgraph da Camada de Armazenamento (MinIO)
    subgraph Storage_Layer ["Storage S3 Compatível"]
        ct-minio[("ct-minio<br/>API: 9000 | Console: 9001")]
    end

    %% Conexão do fluxo de entrada com o Storage
    ct-kafka-consumer -->|Grava arquivos brutos na RAW| ct-minio

    %% Subgraph de Orquestração (Airflow)
    subgraph Orchestration_Orque ["Orquestração e Processamento"]
        ct-postgres[("ct-postgres<br/>Metadados Airflow")] -->|Depende| ct-airflow-init("ct-airflow-init<br/>Cria DB e User Admin")
        
        ct-airflow-init -->|Sucesso| ct-airflow-webserver["ct-airflow-webserver<br/>Porta UI: 8081"]
        ct-airflow-init -->|Sucesso| ct-airflow-scheduler["ct-airflow-scheduler<br/>Scheduler / DAGs PySpark"]
    end

    %% Relação do Airflow/Spark com as Camadas do Lakehouse
    ct-airflow-scheduler -->|Lê RAW, processa e grava Trusted/Gold| ct-minio

    %% Subgraph de Consumo (Analytics / Serving)
    subgraph Analytics_Serving ["Camada de Consumo e Analytics"]
        ct-dremio["ct-dremio<br/>Virtualização SQL: 9047"]
        ct-visual["ct-visual<br/>Dashboard Streamlit: 8501"]
    end

    %% Conexões de consumo de dados
    ct-minio -->|Fornece dados Delta| ct-dremio
    ct-minio -->|Fornece dados para validação| ct-visual

    %% Estilização baseada na Identidade Visual do Ubuntu
    style ct-minio fill:#E95420,stroke:#2C001E,stroke-width:2px,color:#fff
    style ct-kafka fill:#77216F,stroke:#2C001E,stroke-width:2px,color:#fff
    style ct-airflow-scheduler fill:#2C001E,stroke:#77216F,stroke-width:2px,color:#fff
    style ct-airflow-webserver fill:#5E2750,stroke:#2C001E,stroke-width:1px,color:#fff
    style ct-dremio fill:#AEA79F,stroke:#333333,stroke-width:1px,color:#333
    style ct-visual fill:#77216F,stroke:#E95420,stroke-width:1px,color:#fff
    style ct-postgres fill:#AEA79F,stroke:#333333,stroke-width:1px,color:#333
    style ct-zookeeper fill:#AEA79F,stroke:#333333,stroke-width:1px,color:#333

```

---

## 🚀 Como Executar o Projeto

### 1. Pré-requisitos
* Docker e Docker Compose instalados.
* Mínimo de 8GB de RAM (16GB recomendado).

### 2. Passo a Passo
1.  **Clonar o repositório:**
    ```bash
    git clone [https://github.com/ednilsonlomazi/data-pipeline-kafka-airflow-deltalake-docker-tcc.git](https://github.com/ednilsonlomazi/data-pipeline-kafka-airflow-deltalake-docker-tcc.git)
    cd tcc
    ```

2.  **Subir a infraestrutura:**
    ```bash
    sudo docker compose up -d --build
    ```

3.  **Acessar as interfaces:**
    * **Airflow:** `http://localhost:8081` (User: `admin` / Pass: `admin`)
    * **MinIO:** `http://localhost:9001` (User: `admin` / Pass: `password123`)


4.  **Executar as DAGs:**
    * **pipeline_trusted_tab_trafego:** Coleta mensagens de tópicos Kafka unindo em uma única Delta Table, a tab_trafego 
    * **refined_gera_dimensoes:** Coleta a tab_trafego e gera as dimensões do modelo
    * **refined_gera_fatos:** Coleta a tab_trafego e gera a fato

    * **trusted_deltatables_maintenance:** Dag criada para aplicar a otimização de arquivos parquet com Z-Order (agendada para todos os diais 00h)


