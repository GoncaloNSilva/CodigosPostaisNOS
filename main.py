# Press Ctrl+F5 to execute it or replace it with your code.
import csv
import datetime
import time
import json
from json import JSONDecodeError
from timeit import Timer
from unittest.mock import right
import psycopg2
import requests
from psycopg2 import Error

def main():
    call_api("2520-193")
    insert_db()
    # result_csv = read_and_update_csv()

def call_api(codigo_postal):
    try:
        call_URL = "https://www.cttcodigopostal.pt/api/v1/bfff75055c334aa1a1329ffdd5e8811d/"+ codigo_postal
        response = requests.get(call_URL)
        if codigo_postal == "2520-193":
            pretty_print_json(response)
        return response
    except JSONDecodeError:
        print("Erro a descodificar este JSON:")
        pretty_print_json(response)



def pretty_print_json(response):
    parsed = response.json()
    print(json.dumps(parsed))
    print(json.dumps(parsed, indent=4))




def read_and_update_csv():
    full_buff = []
    with open('codigos_postais.csv', 'r') as csvfile:
        parsed = csv.reader(csvfile, delimiter=" ")
        header = next(parsed)  # Read the header
        full_buff.append(header)
        next(parsed)
        for row in parsed:
            row_parsed = row[0].split(",")
            new_row = update_row(row_parsed)
            if new_row is not None:
                full_buff.append(new_row)
            else:
                full_buff.append(row) # Adicionamos a linha original inalterada ao ficheiro caso a API não responda
                print(f"API não retornou quaisquer dados para código postal: {row_parsed[0]}")
    csvfile.close()
    # Write all updated rows back to the CSV file
    with open('codigos_postais.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(full_buff)
    csvfile.close()




def update_row(row_parsed):
    try:
        global last_good_get
        response = call_api(row_parsed[0])
        # Para permitir que o programa espere para repetir pedidos à API
        if response.status_code != 400:
            last_good_get = datetime.datetime.now()
        else:
            while response.status_code == 400:
                secs_elasped = (datetime.datetime.now() - last_good_get).total_seconds()
                time_left = 60 -secs_elasped
                if time_left > 0:
                    print(f"Esperando {time_left:.2f} segundos antes de repetir pedido à API.")
                    time.sleep(time_left)
                    print(f"Repetindo pedido à API com código postal: {row_parsed[0]}")
                    response = call_api(row_parsed[0])
        try:
            parsed = response.json()
        except requests.exceptions.JSONDecodeError:
            # If the response can't be parsed as JSON, try decoding it manually
            decoded_response = response.content.decode('utf-8')
            parsed = json.loads(decoded_response)
        if len(parsed) > 0 and isinstance(parsed[0], dict):
            # Os dados inicialmente vêm como uma lista de dicionários com um par de chave-valor cada apenas
            # a linha seguinte unifica todos os pares num só dicionário utilizando dictionary comprehension
            combined_dict = {k: v for d in parsed for k, v in d.items()}
            for key,value in combined_dict.items(): # vários JSON???
                if(key == "concelho"):
                    row_parsed[1] = value
                if(key == "distrito"):
                    row_parsed[2] = value
            return [",".join(row_parsed)]
        else:
            return
    except requests.exceptions.RequestException as e:
        # Handle request-related errors, such as connection issues or timeouts
        print(f"Request error: {e}")
        return None
    except Exception as e:
        # Catch all other exceptions (including JSONDecodeError if decoding fails)
        print(f"Unexpected error: {e}") # API limite de 30 pedidos/minuto.
        return None


def getCodigosValidos():
    result = []
    with open('codigos_postais.csv', 'r') as csvfile:
        parsed = csv.reader(csvfile, delimiter=" ")
        next(parsed)
        for line in parsed:
            line = line[0].split(",")
            if line[1] != '':
                result.append(line[0])
    return result



def insert_db():
    try:
        # Connect to an existing database
        connection = psycopg2.connect(user="postgres",
                                      password="12345",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="codigopostaldb")
        cursor = connection.cursor()
        postgres_insert_query1 = """
            INSERT INTO codigospostais (
                localidade, freguesia, concelho, distrito, 
                latitude, longitude, codigo_postal, info_local, 
                codigo_arteria, concelho_codigo, distrito_codigo
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        postgres_insert_query2 = """
                    INSERT INTO codigo_morada (
                        codigo_postal, morada, porta
                    ) VALUES (%s, %s, %s)
                """
        codigos_validos = getCodigosValidos()

        for codigo in codigos_validos:
            try:
                global last_good_get
                response = call_api(codigo)
                # Para permitir que o programa espere para repetir pedidos à API
                if response.status_code != 400:
                    last_good_get = datetime.datetime.now()
                    pretty_print_json(response)
                else:
                    while response.status_code == 400:
                        secs_elasped = (datetime.datetime.now() - last_good_get).total_seconds()
                        time_left = 60 - secs_elasped
                        if time_left > 0:
                            print(f"Esperando {time_left:.2f} segundos antes de repetir pedido à API.")
                            time.sleep(time_left)
                            print(f"Repetindo pedido à API com código postal: {codigo}")
                            response = call_api(codigo)
                try:
                    parsed = response.json()
                    combined_dict = {k: v for d in parsed for k, v in d.items()}
                    return
                except requests.exceptions.JSONDecodeError:
                    # If the response can't be parsed as JSON, try decoding it manually
                    decoded_response = response.content.decode('utf-8')
                    parsed = json.loads(decoded_response)
            except requests.exceptions.RequestException as e:
                # Handle request-related errors, such as connection issues or timeouts
                print(f"Request error: {e}")
                return None
            except Exception as e:
                # Catch all other exceptions (including JSONDecodeError if decoding fails)
                print(f"Unexpected error: {e}")  # API limite de 30 pedidos/minuto.
                return None
        # records_to_insert = [
        #     ('Rua ABC', '123', 'Lisboa', 'Santo António', 'Lisboa', 'Lisboa',
        #      38.716667, -9.133333, '1000-001', 'Perto do café', 'ABC123', 1, 11),
        #     ('Avenida XYZ', '456', 'Porto', 'Cedofeita', 'Porto', 'Porto',
        #      41.14961, -8.61099, '4000-002', 'Em frente ao parque', 'XYZ456', 2, 12),
        #     ('Travessa DEF', '789', 'Coimbra', 'Santo António dos Olivais', 'Coimbra', 'Coimbra',
        #      40.203314, -8.410257, '3000-003', 'Ao lado do supermercado', 'DEF789', 3, 13)
        # ]

        # for record in records_to_insert:
        #     cursor.execute(postgres_insert_query, record)

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)


def basic_db():
    try:
        # Connect to an existing database
        connection = psycopg2.connect(user="postgres",
                                      password="12345",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="codigopostaldb")
        cursor = connection.cursor()
        # Print PostgreSQL details
        print("PostgreSQL server information")
        print(connection.get_dsn_parameters(), "\n")
        create_assoc_table = ''' CREATE TABLE codigo_morada (
            id SERIAL PRIMARY KEY,
            codigo_postal VARCHAR(10),
            morada TEXT,
            porta TEXT
            );
        '''
        alter_codigospostais = ''''ALTER TABLE codigospostais
            DROP COLUMN morada,
            DROP COLUMN porta;'''
        # Executing a SQL query
        cursor.execute(create_assoc_table)
        cursor.execute(alter_codigospostais)
        connection.commit()
        print("Table created successfully in PostgreSQL ")
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()












