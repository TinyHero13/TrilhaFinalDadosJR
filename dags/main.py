from get_data import FormTrilha
from sql_insert import LoadDB
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Yasmim_Abrahao',
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}

def process_load_data():
    """
    Função responsável por retornar os dfs criados no arquivo get_data
    """
    get_data_form = FormTrilha()
    fact_df, dim_satisfaction, dim_team, dim_member_type, dim_user = get_data_form.get_dfs()
    return fact_df, dim_satisfaction, dim_team, dim_member_type, dim_user

def insert_into_db():
    """
    Função responsável por utilizar a função de inserir no bigquery
    """
    fact_df, dim_satisfaction, dim_team, dim_member_type, dim_user = process_load_data()
    load_db = LoadDB()
    load_db.insert_tables(fact_df, dim_satisfaction, dim_team, dim_member_type, dim_user)

with DAG(
    dag_id='pipeline_formulario_trilha',
    default_args=default_args,
    description='Pipeline que pega os dados respondidos no formulário do Codigo Certo, faz um tratamento e joga no BigQuery',
    start_date=datetime(2024, 8, 2, 1),
    schedule_interval=timedelta(hours=3), # De 1 em 1 hora
    concurrency=1,
    max_active_runs=1 
) as dag:

    process_data_task = PythonOperator(
        task_id='process_data_task',
        python_callable=process_load_data
    )

    insert_into_db_task = PythonOperator(
        task_id="insert_into_db_task",
        python_callable=insert_into_db
    )

    process_data_task >> insert_into_db_task