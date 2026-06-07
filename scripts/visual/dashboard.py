import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
import plotly.express as px

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

# --- CAMADA VISUAL: CARDS DE MÉTRICAS GERAIS ---
st.subheader("📌 Indicadores de Volumetria e Consistência (Auditoria)")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(label="Volumetria Fato Receita", value=f"{len(df_receita)} reg")
with col2:
    st.metric(label="Volumetria Fato Despesa", value=f"{len(df_despesa)} reg")
with col3:
    # Valida se existem chaves nulas (o que indicaria quebra de integridade referencial nas SKs)
    sks_validas_rec = df_receita['sk_dim_item_receita'].isnull().sum() == 0 if len(df_receita) > 0 else True
    st.metric(label="Integridade SKs Receita", value="100% OK" if sks_validas_rec else "Falha")
with col4:
    sks_validas_des = df_despesa['sk_dim_favorecido'].isnull().sum() == 0 if len(df_despesa) > 0 else True
    st.metric(label="Integridade SKs Despesa", value="100% OK" if sks_validas_des else "Falha")

st.markdown("---")

# --- CAMADA VISUAL: GRÁFICOS ANALÍTICOS ---
st.subheader("📈 Análise Consolidada dos Dados Financeiros")

tab1, tab2 = st.tabs(["📊 Visão de Despesas", "💰 Visão de Receitas"])

with tab1:
    if not df_despesa.empty:
        # Agrupamento simples para plotagem
        df_despesa_grouped = df_despesa.groupby('ano_particao')['vr_amortizacao'].sum().reset_index()
        
        # Criação do gráfico usando Plotly Express (nativo e bonito no Streamlit)
        fig_despesa = px.bar(
            df_despesa_grouped, 
            x='ano_particao', 
            y='vr_amortizacao',
            title="Evolução do Valor Total Pago em Despesas por Ano",
            labels={'ano_particao': 'Ano de Partição (Gold)', 'vr_amortizacao': 'Total Pago (R$)'},
            color_discrete_sequence=['#EF553B']
        )
        st.plotly_chart(fig_despesa, use_container_width=True)
        
        # Mostra uma amostra da tabela fato consolidada com as SKs em MD5
        st.write("**Amostra de Integridade Dimensional (Fato Despesa):**")
        st.dataframe(df_despesa.head(5), use_container_width=True)
    else:
        st.warning("Aguardando ingestão de dados na tabela fato_despesa.")

with tab2:
    if not df_receita.empty:
        df_receita_grouped = df_receita.groupby('ano_particao')['val_efetivado'].sum().reset_index()
        
        fig_receita = px.line(
            df_receita_grouped, 
            x='ano_particao', 
            y='val_efetivado',
            title="Evolução da Arrecadação de Receitas por Ano",
            labels={'ano_particao': 'Ano de Partição (Gold)', 'val_efetivado': 'Total Efetivado (R$)'},
            markers=True
        )
        st.plotly_chart(fig_receita, use_container_width=True)
        
        st.write("**Amostra de Integridade Dimensional (Fato Receita):**")
        st.dataframe(df_receita.head(5), use_container_width=True)
    else:
        st.warning("Aguardando ingestão de dados na tabela fato_receita.")