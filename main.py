# Press Ctrl+F5 to execute it or replace it with your code.
import csv
import datetime
import time
import json
from decimal import Decimal
import psycopg2
import requests
from psycopg2 import Error

def main():
    # call_api("2520-193")
    # result_csv = read_and_update_csv()
    insert_db()
    remove_duplicates_from_db()

def call_api(codigo_postal):
    call_URL = "https://www.cttcodigopostal.pt/api/v1/bfff75055c334aa1a1329ffdd5e8811d/"+ codigo_postal
    response = requests.get(call_URL)
    if codigo_postal == "2520-193": # apenas para demonstração do formato JSON recebido
        pretty_print_json(response)
    return response



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
            # Descodificamos manualmente caso o JSON não consiga descodificar automaticamente
            decoded_response = response.content.decode('utf-8')
            parsed = json.loads(decoded_response)
        if len(parsed) > 0 and isinstance(parsed[0], dict):
            # Os dados inicialmente vêm como uma lista de dicionários com um par de chave-valor cada apenas
            # a linha seguinte unifica todos os pares num só dicionário utilizando dictionary comprehension
            combined_dict = {k: v for d in parsed for k, v in d.items()}
            for key,value in combined_dict.items():
                if(key == "concelho"):
                    row_parsed[1] = value
                if(key == "distrito"):
                    row_parsed[2] = value
            return [",".join(row_parsed)]
        else:
            return
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None
    except Exception as e:
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
    list_of_dicts = []
    try:
        # Connect to an existing database
        connection = psycopg2.connect(user="postgres",
                                      password="12345",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="codigopostaldb")
        cursor = connection.cursor()
        postgres_insert_main = """
            INSERT INTO codigospostais (
                localidade, freguesia, concelho, distrito, 
                latitude, longitude, codigo_postal, info_local, 
                concelho_codigo, distrito_codigo
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        postgres_insert_second = """
            INSERT INTO codigo_morada (
                codigo_postal, morada, porta 
                ) VALUES (%s, %s, %s)
        """

        codigos_validos = getCodigosValidos()
        for codigo in codigos_validos:
            global last_good_get
            try:
                response = call_api(codigo)
                # Para permitir que o programa espere para repetir pedidos à API
                if response.status_code != 400:
                    last_good_get = datetime.datetime.now()
                    # pretty_print_json(response)
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
                except requests.exceptions.JSONDecodeError:
                    # If the response can't be parsed as JSON, try decoding it manually
                    decoded_response = response.content.decode('utf-8')
                    parsed = json.loads(decoded_response)

                list_of_dicts.extend(parsed)
                data_for_query = get_data_for_query(list_of_dicts)
                record_main = set()
                record_sec = set()
                for my_tuple in data_for_query:
                    filtered_values_primary = []
                    sec_tuple = (my_tuple[8],my_tuple[0],my_tuple[1])
                    for i in range(len(my_tuple)):
                        if i != 0 and i != 1:
                            filtered_values_primary.append(my_tuple[i])
                    primary_tuple = tuple(filtered_values_primary)
                    # Check and insert into main table
                    if primary_tuple not in record_main:
                        cursor.execute(postgres_insert_main, primary_tuple)
                        record_main.add(primary_tuple)

                    # Check and insert into secondary table
                    if sec_tuple not in record_sec:
                        cursor.execute(postgres_insert_second, sec_tuple)
                        record_sec.add(sec_tuple)

                    connection.commit()
                    print(f"A adicionar codigo postal {sec_tuple[0]} à db")
            except requests.exceptions.RequestException as e:
                # Handle request-related errors, such as connection issues or timeouts
                print(f"Request error: {e}")
                return None
            except Exception as e:
                # Catch all other exceptions (including JSONDecodeError if decoding fails)
                print(f"Unexpected error: {e}")
                return None
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

def remove_duplicates_from_db():
    try:
        # Establish the database connection
        connection = psycopg2.connect(
            user="postgres",
            password="12345",
            host="127.0.0.1",
            port="5432",
            database="codigopostaldb"
        )
        cursor = connection.cursor()

        # Remove duplicates from codigospostais table
        cursor.execute("""
            DELETE FROM codigospostais
            WHERE id IN (
                SELECT id
                FROM (
                    SELECT id,
                           ROW_NUMBER() OVER (
                               PARTITION BY localidade, freguesia, concelho, distrito, 
                                    latitude, longitude, codigo_postal, info_local, 
                                    concelho_codigo, distrito_codigo
                               ORDER BY id
                           ) AS row_num
                    FROM codigospostais
                ) t
                WHERE t.row_num > 1
            );
        """)

        # Remove duplicates from codigo_morada table
        cursor.execute("""
            DELETE FROM codigo_morada
            WHERE id IN (
                SELECT id
                FROM (
                    SELECT id,
                           ROW_NUMBER() OVER (
                               PARTITION BY codigo_postal, morada, porta
                               ORDER BY id
                           ) AS row_num
                    FROM codigo_morada
                ) t
                WHERE t.row_num > 1
            );
        """)

        # Commit the changes
        connection.commit()
        print("Duplicates removed successfully.")

    except (Exception, psycopg2.Error) as error:
        print("Error while removing duplicates:", error)

    finally:
        # Close the database connection
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

# Call the function to remove duplicates
remove_duplicates_from_db()


def get_data_for_query(list_of_dicts):
    list_of_tuples = []
    try:
        for dictionary in list_of_dicts:
            converted_values = []
            for key, value in dictionary.items():
                if key in ["latitude", "longitude"] and value is not None:
                    # Convert to Decimal for higher precision
                    converted_values.append(Decimal(value))
                elif key in ["codigo-arteria"]:
                    continue
                else:
                    converted_values.append(value)
            tuple_created = tuple(converted_values)
            list_of_tuples.append(tuple_created)
    except Exception as e:
        # Catch all other exceptions (including JSONDecodeError if decoding fails)
        print(f"Unexpected error: {e}")
    finally:
        return list_of_tuples


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

def insert_db_testable(connection, cursor, codigos_validos, call_api_func):
    list_of_dicts = []
    last_good_get = datetime.datetime.now()

    postgres_insert_main = """
        INSERT INTO codigospostais (
            localidade, freguesia, concelho, distrito, 
            latitude, longitude, codigo_postal, info_local, 
            concelho_codigo, distrito_codigo
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    postgres_insert_second = """
        INSERT INTO codigo_morada (
            codigo_postal, morada, porta 
            ) VALUES (%s, %s, %s)
    """

    for codigo in codigos_validos:
        try:
            response = call_api_func(codigo)
            if response.status_code != 400:
                last_good_get = datetime.datetime.now()
            else:
                while response.status_code == 400:
                    secs_elapsed = (datetime.datetime.now() - last_good_get).total_seconds()
                    time_left = 60 - secs_elapsed
                    if time_left > 0:
                        print(f"Esperando {time_left:.2f} segundos antes de repetir pedido à API.")
                        time.sleep(time_left)
                    print(f"Repetindo pedido à API com código postal: {codigo}")
                    response = call_api_func(codigo)

            try:
                parsed = response.json()
            except requests.exceptions.JSONDecodeError:
                decoded_response = response.content.decode('utf-8')
                parsed = json.loads(decoded_response)

            list_of_dicts.extend(parsed)
            data_for_query = get_data_for_query(list_of_dicts)
            record_main = set()
            record_sec = set()

            for my_tuple in data_for_query:
                filtered_values_primary = [my_tuple[i] for i in range(len(my_tuple)) if i not in [0, 1]]
                primary_tuple = tuple(filtered_values_primary)
                sec_tuple = (my_tuple[8], my_tuple[0], my_tuple[1])

                if primary_tuple not in record_main:
                    cursor.execute(postgres_insert_main, primary_tuple)
                    record_main.add(primary_tuple)

                if sec_tuple not in record_sec:
                    cursor.execute(postgres_insert_second, sec_tuple)
                    record_sec.add(sec_tuple)

                connection.commit()
                print(f"A adicionar codigo postal {sec_tuple[0]} à db")

        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")

    return list_of_dicts

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
