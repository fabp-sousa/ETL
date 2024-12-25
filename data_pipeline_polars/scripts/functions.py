import polars as pl
import yaml
from sqlalchemy import create_engine, text
import os


class DataProcess:
    def __init__(self, path, type_df):
        """
        Inicializa a classe DataProcess com um caminho de arquivo e tipo de DataFrame.

        :param path: Caminho para o arquivo de dados (csv ou json).
        :param type_df: Tipo de arquivo ('csv', 'json', ou lista em memória).
        """
        self.__path = path
        self.__type_df = type_df
        self.df = self.__read_data()

    # Leitura de arquivos
    def __read_json(self):
        """Lê um arquivo JSON usando Polars."""
        return pl.read_json(self.__path)

    def __read_csv(self):
        """Lê um arquivo CSV usando Polars."""
        return pl.read_csv(self.__path)

    def __read_data(self):
        """Lê os dados com base no tipo especificado."""
        if self.__type_df == 'csv':
            return self.__read_csv()
        elif self.__type_df == 'json':
            return self.__read_json()
        elif self.__type_df == 'list':
            # Para listas em memória, assume que os dados já estão prontos
            self.__path = 'lista em memória'
            return self.__path
        else:
            raise ValueError("Tipo de dado não suportado: use 'csv', 'json' ou 'list'.")
# --------------------------------------------------------------------------------------------------------------------------
    # Integração com Banco de Dados
    @staticmethod
    def get_db_config(docker_compose_path, service_name="postgres"):
        """
        Lê o arquivo docker-compose.yml e obtém as configurações de banco de dados.

        :param docker_compose_path: Caminho para o arquivo docker-compose.yml.
        :param db_service_name: Nome do serviço no arquivo docker-compose.yml.
        :return: String de conexão para o banco de dados PostgreSQL.
        """
        # Ler o arquivo docker-compose.yml
        with open(docker_compose_path, "r") as file:
            docker_compose = yaml.safe_load(file)
            
        # Obter as informações do serviço
        db_service = docker_compose["services"].get(service_name)
        if not db_service:
            raise ValueError(f"Serviço '{service_name}' não encontrado no arquivo docker-compose.yml.")
            
        # Extrair as variáveis de ambiente do serviço
        db_env = db_service.get("environment", {})
        user = db_env.get("POSTGRES_USER", "postgres")
        password = db_env.get("POSTGRES_PASSWORD", "")
        database = db_env.get("POSTGRES_DB", "postgres")
        host = "localhost"  # Padrão para conexões locais
        port = db_service.get("ports", ["5432:5432"])[0].split(":")[0]

        # Criar a string de conexão
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        
        return connection_string


    def insert_data_to_db(self, engine, table_name, df):
        """
        Insere os dados do DataFrame Polars no banco de dados.

        :param engine: Objeto Engine do SQLAlchemy.
        :param table_name: Nome da tabela no banco.
        """
        # Verifica se o DataFrame Polars está vazio
        if df.shape[0] == 0:
            print("O DataFrame está vazio. Nenhum dado foi inserido.")
            return
        
        # Converte o Polars DataFrame para uma lista de dicionários
        data_to_insert = df.write_database(
        table_name=table_name,
        connection=engine,
        engine="sqlalchemy",
        if_table_exists="replace"
        )  

        print(f"{len(data_to_insert)} registros inseridos na tabela '{table_name}'.")


    def process_and_store(self, docker_compose_path, table_name, df, db_service_name='postgres'):
        """
        Processa os dados e armazena no banco de dados.

        :param docker_compose_path: Caminho para o arquivo docker-compose.yml.
        :param table_name: Nome da tabela no banco.
        :param db_service_name: Nome do serviço no docker-compose.yml.
        """
        db_config = self.__get_db_config(docker_compose_path, db_service_name)

        # carrega dados para a tabela com replace
        self.__insert_data_to_db(db_config, table_name, df)