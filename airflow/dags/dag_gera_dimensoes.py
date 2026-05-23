from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone("America/Sao_Paulo")
deltatables = ['dim_tipo_despesa', 'dim_favorecido', 'dim_contrato_divida', 'dim_alinea_receita', 'dim_item_receita', 'dim_origem_receita', 'dim_rubrica_receita', 'dim_unidade_orcamentaria']

default_args = {
    'owner': 'tcc_projeto',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1, tzinfo=local_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'refined_gera_dimensoes',
    default_args=default_args,
    description='Dispara as pipelines de atualização das dimns',
    schedule='0 1 * * *',
    catchup=False,
    tags=['refined', 'dimensoes'],
    max_active_tasks=2, # limita o paralelismo a 2 tasks por vez
) as dag:

    # chama os processos de raw para trusted
    for t in deltatables:

        task_merge_setup = BashOperator(
            task_id=f'Setup-Gera-{t.replace("_", "-")}',
            bash_command=f'python3 /opt/airflow/scripts/modelagem/dimensoes/{t}.py setup'
        )

        task_merge = BashOperator(
            task_id=f'Gera-{t.replace("_", "-")}',
            bash_command=f'python3 /opt/airflow/scripts/modelagem/dimensoes/{t}.py'
        )

        task_merge_setup >> task_merge

        
        