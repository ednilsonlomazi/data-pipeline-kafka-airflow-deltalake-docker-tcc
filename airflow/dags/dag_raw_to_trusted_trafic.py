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
    schedule='0 12-14 * * *',
    catchup=False,
    tags=['TCC', 'Spark'],
) as dag:

    # a task setup garante que a pasta tab_trafego exista antes de iniciar o processo raw_to_trusted
    task_setup = BashOperator(
        task_id='garantir_tabela_trusted',
        bash_command='python3 /opt/airflow/scripts/raw_to_trusted.py setup'
    )

    # chama os processos de raw para trusted
    for t in topicos:
        task_merge = BashOperator(
            task_id=f'processa_topico_{t}',
            bash_command=f'python3 /opt/airflow/scripts/raw_to_trusted.py {t}'
        )

        # cada task do processo raw to trusted depende a task de setup
        task_setup >> task_merge