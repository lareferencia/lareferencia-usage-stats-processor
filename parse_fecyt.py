import re
import csv

# Paso 1: Leer el archivo que contiene los inserts SQL
with open('drupatablal20240627.sql', 'r') as file:
    sql_content = file.read()

# Paso 2: Extraer los campos y valores de cada insert
# Suponiendo que los inserts tienen el formato: INSERT INTO `table_name` VALUES (value1, value2, ...), (value1, value2, ...);
pattern = re.compile(r"INSERT INTO `\w+` VALUES \((.*?)\);", re.DOTALL)
matches = pattern.findall(sql_content)

# Paso 3: Escribir los campos y valores en un archivo CSV
with open('output.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    
    # Escribir los encabezados (campos)
    # Suponiendo que los campos son id, identifier, record
    fields = ['identifier', 'record']
    csv_writer.writerow(fields)
    
    # Escribir los valores
    for match in matches:
        # Separar los diferentes grupos de valores
        values_groups = match.split('),(')
        for group in values_groups:
            # Limpiar los valores y escribir en el CSV
            values = group.replace('(', '').replace(')', '').split(',')
            ## remove the first value
            values = values[1:]
            values[0] = 'oai:dnet:' + values[0].strip()[1:-1]
            csv_writer.writerow([value.strip().strip("'") for value in values])
