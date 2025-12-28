from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone("America/Sao_Paulo")
topicos = ('l01', 'l03', 'l06')

default_args = {
    'owner': 'tcc_projeto',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1, tzinfo=local_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'pipeline_trusted_tab_trafego',
    default_args=default_args,
    description='Gera tab_trafego na camada Trusted do Data Lakehouse',
    schedule='0 6-18 * * *', # roda toda hora, entre as 6h e 18h
    catchup=False, # começa a partir de agora (ignora os schedules passados)
    tags=['TCC', 'Spark'],
) as dag:

    for t in topicos:
        executar_pyspark = BashOperator(
            task_id=f'processa_topico_{t}',
            bash_command=f'python3 /opt/airflow/scripts/raw_to_trusted.py {t}'
        )

    executar_pyspark