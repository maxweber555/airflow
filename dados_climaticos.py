from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from os.path import join
import pandas as pd
from airflow.macros import ds_add


with DAG(
    "dados_climatico",
    start_date=pendulum.datetime(2024, 12, 30, tz="UTC"),
    schedule_interval='0 0 * * 1', # Executar toda segunda feira
) as dag :
    
    tarefa_1 = BashOperator(
        task_id='cria_pasta',
        bash_command ='mkdir -p "/home/maxweber/airflow/semana={{data_interval_end.strftime("%Y-%m-%d")}}"'
       
    )
    
    
    def extrai_dados(data_interval_end):
                
        city = 'Boston'
        key = '5S8MK3KWENHMVZ8M723LU2539'

        url = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
            f'{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv')
        

        dados = pd.read_csv(url)

       
        file_path = f'/home/maxweber/airflow/semana={data_interval_end}/'       

        dados.to_csv(file_path + 'dados_brutos.csv')
        dados[['datetime', 'tempmin', 'temp','tempmax']].to_csv(file_path + 'temperatura.csv')
        dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')


    tarefa_2 = PythonOperator(
        task_id='extrai_dados',
        python_callable=extrai_dados,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}       
        
    )

    # Ordem de execução
    tarefa_1 >> tarefa_2