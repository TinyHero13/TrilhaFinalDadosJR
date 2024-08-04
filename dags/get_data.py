import requests
import pandas as pd
from io import StringIO

class FormTrilha:
    """
    Pipeline utilizada para pegar os dados
    """
    def __init__(self, data_repo='https://api.github.com/repos/68vinicius/TrilhaFinalDadosJR/contents/Data') -> None:
        self.data_repo = data_repo
        self.df = pd.DataFrame()
        self.result = pd.DataFrame()
        self.df_team = pd.DataFrame()
        self.df_member_type = pd.DataFrame()
        self.df_satisfaction = pd.DataFrame()
        self.df_user = pd.DataFrame()
        self.load_and_merge_data()
    
    def load_and_merge_data(self) -> None:
        self.load_data()
        self.transform_df()
    
    def load_data(self) -> None:
        """ 
        Carregando os dados da pasta 'Data' no repositório GitHub.
        """
        response = requests.get(self.data_repo)
        
        if response.status_code == 200:
            files = response.json()
            
            dataframes = []
            
            for file in files:
                if file['type'] == 'file' and file['name'].endswith('.csv'):
                    file_response = requests.get(file['download_url'])
                    
                    if file_response.status_code == 200:
                        csv_data = StringIO(file_response.text)
                        df = pd.read_csv(csv_data)
                        
                        dataframes.append(df)
                        
            if dataframes:
                self.df = pd.concat(dataframes, ignore_index=True).drop_duplicates()
    
    def transform_df(self) -> None:
        """
        Função para fazer os tratamentos dos dataframes e as transformações em fato e dimensões
        Onde dividi em dimensões: satisfação, tipo de membro e atualmente sou
        """

        # Ajustando o tipo de dado de data para ser o mesmo da tabela no Bigquery
        self.df = self.df.map(lambda x: x.strip() if isinstance(x, str) else x)
        self.df['data'] = pd.to_datetime(self.df['data'], errors='coerce').dt.date

        # Verificação de valores nulos
        self.df.dropna(subset=['data'], inplace=True)

        # Como as questões de satisfação possuem opções estáticas, criei manualmente um DF que será responsável por fazer um join com as demais.
        satisfaction_var = {
            'sk': [0, 1, 2],
            'satisfacao': ["satisfeito", "neutro", "insatisfeito"]
        }
        df_satisfaction =  pd.DataFrame(satisfaction_var)
        

        # Criação da dimensão de time, com a criação das sk (surrogate keys)
        df_team = pd.DataFrame(self.df['minha_equipe'].drop_duplicates().reset_index(drop=True)).reset_index()
        df_team.rename(columns={'index': 'sk_minha_equipe'}, inplace=True)
        self.df_team = df_team

        # Criação da dimensão de tipo de membro, com a criação das sk
        df_member_type = pd.DataFrame(self.df['atualmente_sou'].drop_duplicates().reset_index(drop=True)).reset_index()
        df_member_type.rename(columns={'index': 'sk_atualmente_sou'}, inplace=True)
        self.df_member_type = df_member_type

        # Criação da dimensão dos usuários que poderia também guardar e-mail e deveria ser mascarada
        df_user = pd.DataFrame(self.df['nome_completo'].drop_duplicates().reset_index(drop=True)).reset_index()
        df_user.rename(columns={'index': 'sk_usuario'}, inplace=True)
        self.df_user = df_user


        # Dimensões que serão mescladas que possuem valores de satisfação para serem inseridas como sk
        merge_columns = ['reunioes_do_time', 'colaboracao_entre_membros', 'ambiente_de_aprendizagem', 
                         'comunicacao_entre_membros', 'satisfacao_geral_comunidade', 'feedbacks']

        result = self.df.copy()

        for col in merge_columns:
            result = result.merge(df_satisfaction, left_on=col, right_on='satisfacao', how='left', suffixes=[f"_{col}", '_satisfaction'])
            result.rename(columns={f'sk': f'sk_{col}'}, inplace=True)
            result.drop(columns=[col], inplace=True)
        
        df_satisfaction.rename(columns={'sk': 'sk_satisfacao'}, inplace=True)
        self.df_satisfaction = df_satisfaction

        # Fazendo a mesclagem das sk com a tabela fato e retornando apenas as colunas especificadas que serão jogadas no Bigquery
        result = result.merge(df_team, left_on='minha_equipe', right_on='minha_equipe', how='left')
        result = result.merge(df_member_type, left_on='atualmente_sou', right_on='atualmente_sou', how='left')
        result = result.merge(df_user, left_on='nome_completo', right_on='nome_completo', how='left')

        self.result = result[[
            "data","horas_semanais_dedicadas", "comentario_adicional",
            "sk_minha_equipe", "sk_atualmente_sou", "sk_reunioes_do_time", "sk_colaboracao_entre_membros",
            "sk_ambiente_de_aprendizagem", "sk_comunicacao_entre_membros", "sk_satisfacao_geral_comunidade",
            "sk_feedbacks",  "sk_usuario"
        ]]

    def get_dfs(self) -> pd.DataFrame:
        return self.result, self.df_satisfaction, self.df_team, self.df_member_type, self.df_user

if __name__ == "__main__":
    ## Não necessário para ser utilizado no airflow mas apenas para testar essa parte do código
    df = FormTrilha()
    fact_df, dim_satisfaction, dim_team, dim_member_type, dim_user = df.get_dfs()
    print(dim_user.head())
