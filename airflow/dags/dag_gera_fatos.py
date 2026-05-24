from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone("America/Sao_Paulo")
deltatables = ('fato_receita',)

default_args = {
    'owner': 'tcc_projeto',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1, tzinfo=local_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'refined_gera_fatos',
    default_args=default_args,
    description='Dispara as pipelines de atualização das fatos',
    #schedule='0 2 * * *',
    catchup=False,
    is_paused_upon_creation=True, # apos reiniciar o container, a dag nasce pausada
    tags=['refined', 'fatos'],
    max_active_tasks=2, # limita o paralelismo a 2 tasks por vez
) as dag:

    # chama os processos de raw para trusted
    for t in deltatables:
        task_merge = BashOperator(
            task_id=f'Gera-{t.replace("_", "-")}',
            bash_command=f'python3 /opt/airflow/scripts/modelagem/fatos/{t}.py'
        )

        
        