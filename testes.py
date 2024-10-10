import unittest
from unittest import mock
from unittest.mock import Mock, patch
import psycopg2

from main import insert_db_testable
from db_api import read_db_codigopostal

class TestDatabaseConnection(unittest.TestCase):
    def teste_conexao_basedados(self):
        """Verifica se a ligação à base de dados é bem-sucedida."""
        try:
            connection = psycopg2.connect(
                user="postgres",
                password="12345",
                host="127.0.0.1",
                port="5432",
                database="codigopostaldb"
            )

            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            self.assertEqual(result[0], 1, "Teste de conexão à base de dados falhou.")

        except (Exception, psycopg2.Error) as erro:
            self.fail(f"Teste de conexão à base de dados falhou: {erro}")

        finally:
            if connection:
                cursor.close()
                connection.close()

class TestDatabaseOperations(unittest.TestCase):

    @patch('db_api.psycopg2.connect')
    def test_retrieve_postal_code_data(self, mock_connect):
        """Caso de teste falhado: Dados falsos passados para API para o teste falhar deliberadamente."""
        conexao_teste = Mock()
        cursor_teste = Mock()
        mock_connect.return_value = conexao_teste
        conexao_teste.cursor.return_value = cursor_teste

        # Simular retorno vazio vindo da bd
        cursor_teste.fetchall.side_effect = [
            [],  # Primeiro query está vazio
            []   # Segundo query está vazio
        ]

        # Chamar a função e garantir que retorna listas vazias
        codigospostais_data, codigo_morada_data = read_db_codigopostal('7750-104')

        # Verificar que não retorna quaisquer dados para um input vazio
        self.assertEqual(codigospostais_data, [], "O retorno para codigospostais_data devia ser lista vazia.")
        self.assertEqual(codigo_morada_data, [], "O retorno para codigo_morada_data devia ser lista vazia.")

        print("TESTE FALHADO")

    @patch('db_api.psycopg2.connect')
    def test_retrieve_postal_code_data_successful(self, mock_connect):
        """Caso de teste para sucesso: Simulamos uma situação em que os dados devem ser encontrados na bd."""
        mock_conn = Mock()
        cursor_teste = Mock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = cursor_teste

        # Valor esperado para o input dado acima
        cursor_teste.fetchall.side_effect = [
            [('7750-104', 'Rua Catarina Eufémia', '', 'Mina de São Domingos', 'Corte do Pinto', 'Mértola', 'Beja', None, None, '', 9, 2)],  # codigospostais data
            [('7750-104', 'Rua Catarina Eufémia', '')]  # codigo_morada data
        ]

        # Chamar a função e garantir que retorna listas de dados
        codigospostais_data, codigo_morada_data = read_db_codigopostal('7750-104')


        # Verificar se o query gerado pelo psycopg2 está correto
        cursor_teste.execute.assert_any_call(mock.ANY, ('%7750-104%',))
        cursor_teste.execute.assert_any_call(mock.ANY, ('%7750-104%',))

        # Verificar se os dados retornados correspondem aos esperados
        self.assertEqual(codigospostais_data,
                         [('7750-104', 'Rua Catarina Eufémia', '', 'Mina de São Domingos', 'Corte do Pinto', 'Mértola', 'Beja', None, None, '', 9, 2)])
        self.assertEqual(codigo_morada_data, [('7750-104', 'Rua Catarina Eufémia', '')])

class TestInsertDB(unittest.TestCase):

    @patch('main.get_data_for_query')
    def teste_insert_db(self, mock_get_data_for_query):
        """Testa a inserção de dados na base de dados."""
        conexao_teste = Mock()
        cursor_teste = Mock()

        chamada_api_teste = Mock()
        chamada_api_teste.return_value = Mock(status_code=200, json=lambda: [{"some": "data"}])

        mock_get_data_for_query.return_value = [
            ('Rua ABC', '123', 'Lisboa', 'Santo António', 'Lisboa', 'Lisboa', 38.716667, -9.133333, '1000-001',
             'Perto do café', 'ABC123', 1)
        ]

        codigos_validos = ['1000-001', '7750-104']

        result = insert_db_testable(conexao_teste, cursor_teste, codigos_validos, chamada_api_teste)

        self.assertEqual(len(result), 2, "O número de elementos retornados deveria ser 2.")

        cursor_teste.execute.assert_any_call(
            """
        INSERT INTO codigospostais (
            localidade, freguesia, concelho, distrito, 
            latitude, longitude, codigo_postal, info_local, 
            concelho_codigo, distrito_codigo
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
            ('Lisboa', 'Santo António', 'Lisboa', 'Lisboa', 38.716667, -9.133333, '1000-001', 'Perto do café', 'ABC123',
             1)
        )

        cursor_teste.execute.assert_any_call(
            """
        INSERT INTO codigo_morada (
            codigo_postal, morada, porta 
            ) VALUES (%s, %s, %s)
    """,
            ('1000-001', 'Rua ABC', '123')
        )

        # Testar se o commit da base de dados aconteceu
        conexao_teste.commit.assert_called()
        self.assertEqual(chamada_api_teste.call_count, 2, "A chamada da API deveria ter ocorrido duas vezes.")

# Estas classes são para executar os testes e produzir o relatório final
class ResultadosTestes(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.results = []

    def addSuccess(self, test):
        super().addSuccess(test)
        self.results.append((test, 'Sucesso'))

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self.results.append((test, 'Falhou'))

    def addError(self, test, err):
        super().addError(test, err)
        self.results.append((test, 'Erro'))

class RunnerTestes(unittest.TextTestRunner):
    def _makeResult(self):
        return ResultadosTestes(self.stream, self.descriptions, self.verbosity)

    def run(self, test):
        result = super().run(test)
        self._print_final_report(result)
        return result

    def _print_final_report(self, result):
        print("\n--- Relatório Final dos Testes ---\n")
        for test, resultado in result.results:
            # Collect and print test information
            nome_teste = test._testMethodName
            docu_teste = test.shortDescription()
            entradas_teste = getattr(test, 'inputs', 'N/A')  # Utiliza o atributo "inputs" se existir
            print(f"Teste: {nome_teste}")
            print(f"Propósito: {docu_teste}")
            print(f"Resultado: {resultado}")
            print(f"Entradas: {entradas_teste}")
            print(f"---\n")

if __name__ == '__main__':
    unittest.main(testRunner=RunnerTestes(verbosity=2))


'''
--- Relatório Final dos Testes ---

Teste: test_retrieve_postal_code_data
Propósito: Testa a recuperação dos dados para um código postal inexistente (cenário falhado).
Resultado: Sucesso
Entradas: N/A
---

Teste: test_retrieve_postal_code_data_successful
Propósito: Testa a recuperação dos dados para um código postal existente (cenário bem-sucedido).
Resultado: Sucesso
Entradas: N/A
---

Teste: teste_insert_db
Propósito: Testa a inserção de dados na base de dados.
Resultado: Sucesso
Entradas: N/A
---
'''