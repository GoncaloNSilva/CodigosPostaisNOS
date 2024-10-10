import psycopg2


def main():
    print("Por favor insira o código postal para o qual quer consultar a base de dados:", end='')
    codigo_input = input()
    read_db_codigopostal(codigo_input)


def mostrar_resultado(codigospostais_results, codigo_morada_results):
    print("Resultado da pesquisa na base de dados:")
    print(f"\tCódigo Postal: {codigo_morada_results[1]}")
    print(f"\tRua: {codigo_morada_results[2]}")
    print(f"\tPorta: {codigo_morada_results[3]}")
    print(f"\tLocalidade: {codigospostais_results[1]}")
    print(f"\tFreguesia: {codigospostais_results[2]}")
    print(f"\tConcelho: {codigospostais_results[3]}")
    print(f"\tDistrito: {codigospostais_results[4]}")
    print(f"\tLatitude: {codigospostais_results[5]}")
    print(f"\tLongitude: {codigospostais_results[6]}")
    print(f"\tInfo Local: {codigospostais_results[8]}")
    print(f"\tCódigo de Concelho: {codigospostais_results[9]}")
    print(f"\tCódigo de Distrito: {codigospostais_results[10]}")


def read_db_codigopostal(codigo_postal):
    try:
        connection = psycopg2.connect(
            user="postgres",
            password="12345",
            host="127.0.0.1",
            port="5432",
            database="codigopostaldb"
        )
        cursor = connection.cursor()

        # Query para tabela codigospostais
        cursor.execute("""
            SELECT *
            FROM codigospostais
            WHERE codigo_postal LIKE %s;
        """, (f'%{codigo_postal}%',))

        codigospostais_results = cursor.fetchall()

        # Query para tabela codigo_morada
        cursor.execute("""
            SELECT *
            FROM codigo_morada
            WHERE codigo_postal LIKE %s;
        """, (f'%{codigo_postal}%',))

        codigo_morada_results = cursor.fetchall()

        # Caso nenhuns dados para o codigo postal de input
        if not codigospostais_results or not codigo_morada_results:
            print("Nenhuns dados na base de dados para esse código postal.")
            return [], []  # Return empty lists if no data is found

        # Use the first result for both tables
        codigo_morada = list(codigo_morada_results[0])
        codigo_morada[3] = "Não disponível" if codigo_morada[3] == '' else codigo_morada[3]

        codigospostais = list(codigospostais_results[0])
        codigospostais[5] = "Não disponível" if codigospostais[5] is None else codigospostais[5]
        codigospostais[6] = "Não disponível" if codigospostais[6] is None else codigospostais[6]
        codigospostais[8] = "Não disponível" if codigospostais[8] == '' else codigospostais[8]

        # Display results
        mostrar_resultado(codigospostais, codigo_morada)

        return codigospostais_results, codigo_morada_results

    except (Exception, psycopg2.Error) as error:
        print("Error while retrieving data:", error)
        return [], []  # Listas vazias em caso de erro

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
