# Press Ctrl+F5 to execute it or replace it with your code.
import csv
import json
import psycopg2
import requests
from psycopg2 import Error

def main():
    # connection = connect_db()
    api_key = "bfff75055c334aa1a1329ffdd5e8811d"
    codigo_postal = "2520-193"
    call_URL = "https://www.cttcodigopostal.pt/api/v1/" + api_key +"/"+ codigo_postal
    response = requests.get(call_URL)
    pretty_print_JSON(response)
    result_csv = read_and_print_csv()

def pretty_print_JSON(response):
    parsed = response.json()
    print(json.dumps(parsed, indent=4))

def read_and_print_csv():
    with open('codigos_postais.csv', 'r') as csvfile:
        parsed = csv.reader(csvfile, delimiter=" ")
        next(parsed)
        for row in parsed:
            row_parsed = row[0].split(",")
            print(f"[0]:{row_parsed[0]}\n[1]:{row_parsed[1]}\n[2]:{row_parsed[2]}\n")
            return

def connect_db():
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
        create_table_query = ''' CREATE TABLE addresses (
            id SERIAL PRIMARY KEY,
            morada TEXT,
            porta TEXT,
            localidade TEXT,
            freguesia TEXT,
            concelho TEXT,
            distrito TEXT,
            latitude DECIMAL(10, 7),
            longitude DECIMAL(10, 7),
            codigo_postal VARCHAR(10),
            info_local TEXT,
            codigo_arteria VARCHAR(20),
            concelho_codigo INTEGER,
            distrito_codigo INTEGER);
        '''
        # Executing a SQL query
        cursor.execute(create_table_query)
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

