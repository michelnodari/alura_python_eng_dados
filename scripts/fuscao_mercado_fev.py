import json
import csv

#---------------------------------FUNÇÕES
def leitura_json(path_json):
    try:
        with open(path_json, 'r') as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Erro ao ler o arquivo JSON: {e}")
        return []

def leitura_csv(path_csv):
    dados_csv = []
    try:
        with open(path_csv, 'r') as file:
            spamreader = csv.DictReader(file, delimiter=',')
            for row in spamreader:
                dados_csv.append(row)
    except (FileNotFoundError, csv.Error) as e:
        print(f"Erro ao ler o arquivo CSV: {e}")
    return dados_csv

def leitura_dados(path, tipo_arquivo):
    if tipo_arquivo == 'csv':
        return leitura_csv(path)
    elif tipo_arquivo == 'json':
        return leitura_json(path)
    return []

def get_colunas(dados):
    return list(dados[0].keys()) if dados else []

def rename_columns(dados, key_mapping):
    new_dados_csv = []
    for old_dict in dados:
        dict_temp = {key_mapping.get(old_key, old_key): value for old_key, value in old_dict.items()}
        new_dados_csv.append(dict_temp)
    return new_dados_csv

def size_data(dados):
    return len(dados)

def union_dados(dadosA, dadosB):
    return dadosA + dadosB

def convert_dados_tabela(dados, nome_colunas):
    dados_unificados_tabela = [nome_colunas]
    for row in dados:
        dados_unificados_tabela.append([row.get(coluna, '') for coluna in nome_colunas])
    return dados_unificados_tabela

def export_dados_fusao(dados, path):
    try:
        with open(path, 'w', newline='') as file:        
            writer = csv.writer(file, delimiter=';')
            writer.writerows(dados)
    except IOError as e:
        print(f"Erro ao escrever o arquivo CSV: {e}")

###############################################################
path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'

# Inicia a leitura dos dados
dados_json = leitura_dados(path_json, 'json')
nome_colunas_json = get_colunas(dados_json)
tamanho_dados_json = size_data(dados_json)
print(f"Nome das Colunas Leitura JSON: {nome_colunas_json}")
print(f"Primeira Linha JSON: {dados_json[0] if dados_json else 'Vazio'}")
print(f"Total de Linhas JSON: {tamanho_dados_json}")

dados_csv = leitura_dados(path_csv, 'csv')
nome_colunas_csv = get_colunas(dados_csv)
tamanho_dados_csv = size_data(dados_csv)
print(f"Nome das Colunas Leitura CSV: {nome_colunas_csv}")
print(f"Primeira Linha CSV: {dados_csv[0] if dados_csv else 'Vazio'}")
print(f"Total de Linhas CSV: {tamanho_dados_csv}")

# Transformação dos dados
key_mapping = {
    'Nome do Item': 'Nome do Produto',
    'Classificação do Produto': 'Categoria do Produto',
    'Valor em Reais (R$)': 'Preço do Produto (R$)',
    'Quantidade em Estoque': 'Quantidade em Estoque',
    'Nome da Loja': 'Filial',
    'Data da Venda': 'Data da Venda'
}

dados_csv = rename_columns(dados_csv, key_mapping)
nome_colunas_csv = get_colunas(dados_csv)
print(f"Colunas CSV Renomeadas: {nome_colunas_csv}")

dados_fusao = union_dados(dados_csv,dados_json)
nome_colunas_fusao = get_colunas(dados_fusao)
tamanho_dados_fusao = size_data(dados_fusao)
print(f"Nome das Colunas Tabela Fusão: {nome_colunas_fusao}")
print(f"Total de Linhas Tabela Fusão: {tamanho_dados_fusao}")

# Salvando dados
tb_dados_fusao = convert_dados_tabela(dados_fusao, nome_colunas_fusao)
print(f"Dados para Exportação: {tb_dados_fusao[:5]}")  # Imprime as primeiras 5 linhas para verificação

path_export_dados_fusao = 'data_processed/tb_dados_fusao.csv'
export_dados_fusao(tb_dados_fusao, path_export_dados_fusao)
print(f"Dados exportados para: {path_export_dados_fusao}")
