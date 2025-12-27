from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

topicos = ('l01', 'l03', 'l06')

default_args = {
    'owner': 'tcc_projeto',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'pipeline_raw_to_trusted',
    default_args=default_args,
    description='Executa o script PySpark de limpeza',
    schedule_interval=None, # Rodaremos manualmente pelo painel
    catchup=False,
    tags=['TCC', 'Spark'],
) as dag:

    for t in topicos:
        executar_pyspark = BashOperator(
            task_id=f'processa_topico_{t}',
            bash_command=f'python3 /opt/airflow/scripts/raw_to_trusted.py {t}'
        )

    executar_pyspark