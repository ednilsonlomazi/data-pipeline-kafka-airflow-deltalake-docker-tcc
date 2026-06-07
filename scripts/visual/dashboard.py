import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession

# Configuração da página do Streamlit
st.set_page_config(
    page_title="Data Lakehouse Analytics - Validação Refined",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Painel de Validação Analítica - Camada Refined")
st.markdown("""
Este painel valida a prontidão analítica da camada **Refined (Gold)** do Modern Data Lakehouse, 
comprovando a integridade das *Surrogate Keys* e a correta consolidação dos dados processados via DataOps.
""")

@st.cache_data(ttl=300) # Mantém em cache por 5 minutos para evitar reabrir o Spark toda hora
def carregar_dados_lakehouse():
    # 1. Definição das versões exatas que você usa no projeto
    DELTA_PKG = "io.delta:delta-spark_2.12:3.0.0"
    HADOOP_AWS_PKG = "org.apache.hadoop:hadoop-aws:3.3.4"
    AWS_SDK_PKG = "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    PACKAGES = f"{DELTA_PKG},{HADOOP_AWS_PKG},{AWS_SDK_PKG}"

    # 2. Inicializa a SparkSession injetando os pacotes obrigatórios
    spark = SparkSession.builder \
        .appName("Streamlit-Lakehouse-Validation") \
        .config("spark.jars.packages", PACKAGES) \
        .config("spark.jars.ivy", "/tmp/.ivy2") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://ct-minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    try:
        # Lendo as duas Tabelas Fato da Refined
        df_receita_spark = spark.read.format("delta").load("s3a://refined/fato_receita")
        df_despesa_spark = spark.read.format("delta").load("s3a://refined/fato_despesa")
        
        # Converte para Pandas para o Streamlit manipular de forma leve
        df_receita = df_receita_spark.toPandas()
        df_despesa = df_despesa_spark.toPandas()
        
        # Garante que as colunas de valores sejam numéricas no Pandas (evita falhas de soma)
        if not df_receita.empty:
            df_receita['val_efetivado'] = pd.to_numeric(df_receita['val_efetivado'], errors='coerce')
        if not df_despesa.empty:
            df_despesa['vr_amortizacao'] = pd.to_numeric(df_despesa['vr_amortizacao'], errors='coerce')
            
    except Exception as e:
        st.error(f"Erro ao conectar com a camada Refined no MinIO: {e}")
        # Retorna dataframes vazios simulados caso o MinIO esteja fora do ar no momento
        df_receita = pd.DataFrame(columns=['ano_particao', 'val_efetivado', 'sk_dim_item_receita'])
        df_despesa = pd.DataFrame(columns=['ano_particao', 'vr_amortizacao', 'sk_dim_favorecido'])
    finally:
        spark.stop() # Encerra a sessão para liberar memória do container
        
    return df_receita, df_despesa

# Carregando os dados
with st.spinner("Conectando ao MinIO e extraindo tabelas Delta..."):
    df_receita, df_despesa = carregar_dados_lakehouse()

# --- CAMADA VISUAL: CARDS DE AUDITORIA E VOLUMETRIA ---
st.subheader("📌 Indicadores de Volumetria e Consistência (Auditoria)")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(label="Volumetria Fato Receita", value=f"{len(df_receita):,} reg".replace(",", "."))
with col2:
    st.metric(label="Volumetria Fato Despesa", value=f"{len(df_despesa):,} reg".replace(",", "."))
with col3:
    sks_validas_rec = df_receita['sk_dim_item_receita'].isnull().sum() == 0 if len(df_receita) > 0 else True
    st.metric(label="Integridade SKs Receita", value="100% OK" if sks_validas_rec else "Falha")
with col4:
    sks_validas_des = df_despesa['sk_dim_favorecido'].isnull().sum() == 0 if len(df_despesa) > 0 else True
    st.metric(label="Integridade SKs Despesa", value="100% OK" if sks_validas_des else "Falha")

st.markdown("---")

# --- CAMADA VISUAL: CARDS DE VALORES NUMÉRICOS (SOLICITADO) ---
st.subheader("💰 Totais Consolidados Financeiros")
m_col1, m_col2 = st.columns(2)

with m_col1:
    total_receita = df_receita['val_efetivado'].sum() if not df_receita.empty else 0.0
    st.metric(
        label="Valor Total Efetivado (Receitas)", 
        value=f"R$ {total_receita:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    )

with m_col2:
    total_despesa = df_despesa['vr_amortizacao'].sum() if not df_despesa.empty else 0.0
    st.metric(
        label="Valor Total Amortizado (Despesas)", 
        value=f"R$ {total_despesa:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    )

st.markdown("---")

# --- CAMADA VISUAL: SÉRIES DE DADOS (ABAS COM TABELAS) ---
st.subheader("📋 Visualização dos Dados Brutos Consolidados")
tab1, tab2 = st.tabs(["📊 Tabela de Despesas", "💰 Tabela de Receitas"])

with tab1:
    if not df_despesa.empty:
        st.write("**Amostra de Integridade Dimensional (Fato Despesa):**")
        st.dataframe(df_despesa.head(100), use_container_width=True)
    else:
        st.warning("Aguardando ingestão de dados na tabela fato_despesa.")

with tab2:
    if not df_receita.empty:
        st.write("**Amostra de Integridade Dimensional (Fato Receita):**")
        st.dataframe(df_receita.head(100), use_container_width=True)
    else:
        st.warning("Aguardando ingestão de dados na tabela fato_receita.")