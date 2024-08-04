from google.cloud import bigquery
from google.oauth2 import service_account
from airflow.hooks.base_hook import BaseHook
import pandas as pd

def get_google_cloud_credentials(conn_id='bigquery_conn'):
    """
    Pegando o keyfile_json que foi gerado dentro do bigquery e foi salvo localmente para ser utilizado pelo airflow.
    """
    connection = BaseHook.get_connection(conn_id)
    keyfile_dict = connection.extra_dejson.get('keyfile_dict')
    keyfile_json = eval(keyfile_dict) 
    credentials = service_account.Credentials.from_service_account_info(keyfile_json)
    return credentials

class LoadDB:
    def __init__(self, conn_id='bigquery_conn') -> None:
        
        self.credentials = get_google_cloud_credentials(conn_id)
        self.client = bigquery.Client(credentials=self.credentials)
      
    def inserting_df(self, df, table_id) -> None:
        """
        Função responsável por não repetir o mesmo código no insert tables e inserir cada df e id da tabela no Bigquery
        """
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

    def insert_tables(self, fact_df, dim_satisfaction_df, 
                      dim_team_df, dim_member_type_df, 
                      dim_user_df) -> None:
        """
        Função responsável por utilizar a função de inserção dos dfs (fato e dimensões) dentro do bigquery
        """
        # As tabelas fato e dimensões que foram criadas no Bigquery
        fact_table = "trilhajrprojeto.codigocertodados.fato_formulario"
        dim_satisfaction_table = "trilhajrprojeto.codigocertodados.dim_satisfaction"
        dim_team_table = "trilhajrprojeto.codigocertodados.dim_team"
        dim_member_type_table = "trilhajrprojeto.codigocertodados.dim_member_type"
        dim_user_table = "trilhajrprojeto.codigocertodados.dim_user"

        self.inserting_df(fact_df, fact_table)
        self.inserting_df(dim_satisfaction_df, dim_satisfaction_table)
        self.inserting_df(dim_team_df, dim_team_table)
        self.inserting_df(dim_member_type_df, dim_member_type_table)
        self.inserting_df(dim_user_df, dim_user_table)
