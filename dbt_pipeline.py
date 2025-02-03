from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 24),
    'retries': 1,
}


with DAG(
    'pipeline_elt',
    default_args=default_args,
    description='Executa dbt run e dbt test para Snowflake',
    schedule_interval='0 0 * * *',  
    catchup=False,
) as dag:


    start = BashOperator(
        task_id='start',
        bash_command="echo 'DAG Iniciada'"  
    )


    venv_path = "/mnt/c/Users/user/Desktop/isa/desafio_LH/desafio_LH/venv/bin/activate"

    
    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=f"source {venv_path} && cd /mnt/c/Users/user/Desktop/isa/desafio_LH/desafio_LH/models/staging && dbt run --models staging",  # Ativa o venv antes de rodar o dbt
    )

    
    dbt_test_staging = BashOperator(
        task_id='dbt_test_staging',
        bash_command=f"source {venv_path} && cd /mnt/c/Users/user/Desktop/isa/desafio_LH/desafio_LH/models/staging && dbt test --models staging", 
    )

    
    dbt_run_mart = BashOperator(
        task_id='dbt_run_mart',
        bash_command=f"source {venv_path} && cd /mnt/c/Users/user/Desktop/isa/desafio_LH/desafio_LH/models/marts && dbt run --models marts", 
    )

    
    dbt_test_mart = BashOperator(
        task_id='dbt_test_mart',
        bash_command=f"source {venv_path} && cd /mnt/c/Users/user/Desktop/isa/desafio_LH/desafio_LH/models/marts && dbt test --models marts",  
    )

    
    start >> dbt_run_staging >> dbt_test_staging >> dbt_run_mart >> dbt_test_mart
