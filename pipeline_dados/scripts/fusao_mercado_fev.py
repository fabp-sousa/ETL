# imports
from processamento_dados import Dados

# Extract and read data
# variables
path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'

dados_empresaA = Dados(path_json,'json')
dados_empresaB = Dados(path_csv, 'csv')

print(f'columns A: {dados_empresaA.nome_colulnas}')

print(f'columns B: {dados_empresaB.nome_colulnas}')

# Transform

key_mapping = {"Nome do Item":"Nome do Produto",
            "Classificação do Produto": "Categoria do Produto",
            "Valor em Reais (R$)":"Preço do Produto (R$)",
            "Nome da Loja":"Filial",
            "Data da Venda":"Data da Venda",
            "Quantidade em Estoque":"Quantidade em Estoque"}

dados_empresaB.rename_columns(key_mapping)
print(f'rename B comlumns: {dados_empresaB.nome_colulnas}')

print(f'qtde de registros Empresa A: {dados_empresaA.size_data()}')
print(f'qtde de registros Empresa B: {dados_empresaB.size_data()}')


dados_fusao = Dados.join_df(dados_empresaB, dados_empresaA)
print(f'Colunas da funçao da fusao: {dados_fusao.nome_colulnas}')
print(f'Qtde de registros na fusão: {dados_fusao.size_data()}')

# Load
path_data_combined = 'data_process/dados_combinados_classe.csv'

dados_fusao.load_data(path_data_combined)
print(path_data_combined)