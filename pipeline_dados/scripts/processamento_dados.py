import json
import csv 

class Dados:

    def __init__(self, path, type_df):
        self.__path = path
        self.__type_df = type_df
        self.df = self.__read_data()
        self.nome_colulnas = self.__get_columns()
        self.qtde_linhas = self.size_data()


    # functions
    def __read_json(self):
        dados_jason = []
        with open(self.__path, 'r') as file:
            dados_jason = json.load(file)

        return dados_jason


    def __read_csv(self):
        dados_csv =[]
        with open(self.__path, 'r') as file:
            spamreader = csv.DictReader(file, delimiter=',')
            for row in spamreader:
                dados_csv.append(row)

        return dados_csv


    def __read_data(self):
        df = []

        if self.__type_df == 'csv':
            df = self.__read_csv()
        
        elif self.__type_df == 'json':
            df = self.__read_json()

        elif self.__type_df == 'list':
            df = self.__path
            self.__path = 'lista em memoria'

        return df


    def __get_columns(self):
        columns_names = list(self.df[0].keys())
        return columns_names
    

    def rename_columns(self, key_mapping):
        new_df = []

        for old_dict in self.df:
            dict_temp = {}
            for old_key, value in old_dict.items():
                dict_temp[key_mapping[old_key]] = value
            
            new_df.append(dict_temp)

        self.df = new_df
        self.nome_colulnas = self.__get_columns()


    def size_data(self):
        return len(self.df)
    

    def join_df(df_a, df_b):
        combined_list =[]
        combined_list.extend(df_a.df)
        combined_list.extend(df_b.df)

        return Dados(combined_list, 'list')


    def __transform_data(self):
        df_combined = [self.nome_colulnas]

        for row in self.df:
            row_i = []
            for column in self.nome_colulnas:
                row_i.append(row.get(column, 'indisponivel'))
            df_combined.append(row_i)

        return df_combined
    

    def load_data(self, path):

        df_combined = self.__transform_data()
        with open(path, 'w') as file:
            writer = csv.writer(file)
            writer.writerows(df_combined)