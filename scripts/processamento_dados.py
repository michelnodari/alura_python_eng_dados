import json
import csv

class Dados:
    def __init__(self, path, tipo_dados):
        self.path = path
        self.tipo_dados = tipo_dados
        self.dados = self.leitura_dados()
        self.nome_colunas = self.get_colunas()
        self.qtd_linhas = self.size_data()

    def leitura_json(self):
        """Lê um arquivo JSON e retorna os dados."""
        try:
            with open(self.path, 'r') as file:
                return json.load(file)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Erro ao ler o arquivo JSON: {e}")
        return []

    def leitura_csv(self):
        """Lê um arquivo CSV e retorna os dados."""
        dados_csv = []
        try:
            with open(self.path, 'r') as file:
                spamreader = csv.DictReader(file, delimiter=',')
                for row in spamreader:
                    dados_csv.append(row)
        except (FileNotFoundError, csv.Error) as e:
            print(f"Erro ao ler o arquivo CSV: {e}")
        return dados_csv

    def leitura_dados(self):
        """Lê os dados com base no tipo especificado."""
        dados = []
        if self.tipo_dados == 'csv':
            dados = self.leitura_csv()
        elif self.tipo_dados == 'json':
            dados = self.leitura_json()
        elif self.tipo_dados == 'list':
            dados = self.path  # Assume que path é uma lista de dicionários
        return dados

    def get_colunas(self):
        """Obtém os nomes das colunas dos dados."""
        return list(self.dados[0].keys()) if self.dados else []

    def rename_columns(self, key_mapping):
        """Renomeia as colunas de acordo com um mapeamento fornecido."""
        if not isinstance(key_mapping, dict):
            print("Erro: key_mapping deve ser um dicionário")
            return

        new_dados = []
        for old_dict in self.dados:
            dict_temp = {key_mapping.get(old_key, old_key): value for old_key, value in old_dict.items()}
            new_dados.append(dict_temp)
        self.dados = new_dados
        self.nome_colunas = self.get_colunas()

    def size_data(self):
        """Retorna a quantidade de linhas nos dados."""
        return len(self.dados)

    def union_dados(self, dadosB, dadosA):
        """Une os dados de dois objetos Dados."""
        unificacao_list = []
        unificacao_list.extend(dadosB.dados)
        unificacao_list.extend(dadosA.dados)
        return Dados(unificacao_list, 'list')
    
    def convert_dados_tabela(self):
        """Converte os dados para uma tabela."""
        dados_unificados_tabela = [self.nome_colunas]
        for row in self.dados:
            dados_unificados_tabela.append([row.get(coluna, '') for coluna in self.nome_colunas])
        return dados_unificados_tabela

    def export_dados_fusao(self, path):
        """Exporta os dados unificados para um arquivo CSV."""
        try:
            dados_combinados_tabela = self.convert_dados_tabela()
            with open(path, 'w', newline='') as file:
                writer = csv.writer(file, delimiter=';')
                writer.writerows(dados_combinados_tabela)
        except IOError as e:
            print(f"Erro ao escrever o arquivo CSV: {e}")

#####################################################################

