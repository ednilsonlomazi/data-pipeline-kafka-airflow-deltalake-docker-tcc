from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone("America/Sao_Paulo")
deltatables = ('tab_trafego',)

default_args = {
    'owner': 'tcc_projeto',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1, tzinfo=local_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'trusted_deltatables_maintenance',
    default_args=default_args,
    description='Agrupamento de arquivos por Z-Order',
    #schedule='0 0 * * *',
    is_paused_upon_creation=True, # apos reiniciar o container, a dag nasce pausada
    catchup=False,
    tags=['maintenance', 'trusted', 'deltatables'],
    max_active_tasks=1, # cancela paralelisto de tasks, roda uma por vez
) as dag:



    # chama os processos de raw para trusted
    for t in deltatables:
        task_merge = BashOperator(
            task_id=f'Aplica-Z-Order-em-{t.replace("_", "-")}',
            bash_command=f'python3 /opt/airflow/scripts/trusted_maintenance.py {t}'
        )

        # cada task do processo raw to trusted depende a task de setup
        