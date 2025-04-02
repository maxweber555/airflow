from airflow import DAG
from airflow.providers.oracle.operators.oracle import OracleOperator
from datetime import datetime, timedelta

# Configuração padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG
with DAG(
    dag_id='dag_taxmaoc_elastic_regras',
    default_args=default_args,
    description='Executa o procedimento taxmaoc.stp_elastic_regras diariamente',
    schedule_interval='@daily',  # Executa diariamente
    start_date=datetime(2025, 3, 27),
    catchup=False,
    tags=['oracle', 'taxmaoc'],
) as dag:

    # Tarefa para executar o procedimento no Oracle
    executar_procedimento = OracleOperator(
        task_id='executar_taxmaoc_stp_elastic_regras',
        oracle_conn_id='OracleDB_prod',  # Nome da conexão configurada no Airflow
        sql='BEGIN taxmaoc.stp_elastic_regras; END;',
    )

    executar_procedimento
