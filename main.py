# Press Ctrl+F5 to execute it or replace it with your code.
import csv
import json
from configparser import ConfigParser

import requests




def main():
    config = load_config()
    # Use a breakpoint in the code line below to debug your script.
    api_key = "bfff75055c334aa1a1329ffdd5e8811d"
    codigo_postal = "2520-193"
    call_URL = "https://www.cttcodigopostal.pt/api/v1/" + api_key +"/"+ codigo_postal
    response = requests.get(call_URL)
    pretty_print_JSON(response)
    read_and_print_csv()

def pretty_print_JSON(response):
    parsed = response.json()
    print(json.dumps(parsed, indent=4))

def read_and_print_csv():
    with open('codigos_postais.csv', 'r') as csvfile:
        parsed = csv.reader(csvfile, delimiter=" ")
        for row in parsed:
            print(f"Dados lidos do ficheiro csv: {parsed}")

def load_config(filename='db.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return config

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

