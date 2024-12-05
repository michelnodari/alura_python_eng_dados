from processamento_dados import Dados

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'

#EXTRACT

dados_empresaA = Dados(path_json, 'json')
print(f"Colunas Empresa A: {dados_empresaA.nome_colunas}")
print(f"Total de linhas Empresa A: {dados_empresaA.qtd_linhas}")

dados_empresaB = Dados(path_csv, 'csv')
print(f"Colunas Empresa B: {dados_empresaB.nome_colunas}")
print(f"Total de linhas Empresa B: {dados_empresaB.qtd_linhas}")

#TRANSFORM DADOS

key_mapping = {
    'Nome do Item': 'Nome do Produto',
    'Classificação do Produto': 'Categoria do Produto',
    'Valor em Reais (R$)': 'Preço do Produto (R$)',
    'Quantidade em Estoque': 'Quantidade em Estoque',
    'Nome da Loja': 'Filial',
    'Data da Venda': 'Data da Venda'
}

dados_empresaB.rename_columns(key_mapping)
print(f"Colunas Empresa B Renomeadas para: {dados_empresaB.nome_colunas}")

# Crie uma instância da classe Dados para chamar union_dados
dados_fusao = dados_empresaB.union_dados(dados_empresaB,dados_empresaA)
print(f"Total de linhas após união: {dados_fusao.qtd_linhas}")
print(f"Colunas após união: {dados_fusao.nome_colunas}")

# LOAD DADOS

path_export_dados_fusao = 'data_processed/tb_dados_fusao_new.csv'

# Exporta os dados unificados para um arquivo CSV
dados_fusao.export_dados_fusao(path_export_dados_fusao)

print(path_export_dados_fusao)