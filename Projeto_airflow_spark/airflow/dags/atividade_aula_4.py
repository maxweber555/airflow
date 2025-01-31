from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Função Python que será executada pelo PythonOperator
def cumprimentos():
        print("Boas-vindas ao Airflow!")


with DAG(
     'atividade_aula_4',
     start_date =days_ago(1),
     schedule_interval='@daily',
     description='Exemplo de DAG com PythonOperator'
) as dag:

    tarefa_1 = EmptyOperator(task_id='tarefa_1')
    tarefa_2 = EmptyOperator(task_id='tarefa_2')
    tarefa_3 = EmptyOperator(task_id='tarefa_3')
    tarefa_4 = PythonOperator(
        task_id='Printar_Boa_Vindas',
        python_callable=cumprimentos
    )
    

    tarefa_1 >> [tarefa_2,tarefa_3]
    tarefa_3 >> tarefa_4
    tarefa_4