import json
import csv
import os

def generate(schema):
    # Create a directory to store CSV files
    output_dir = 'csv_output'
    os.makedirs(output_dir, exist_ok=True)

    # Iterate over each table in the schema
    for table in schema['tables']:
        table_name = table['name']
        file_path = os.path.join(output_dir, f'{table_name}.csv')

        # Extract column names
        column_names = [column['name'] for column in table['columns']]

        # Write to CSV file
        with open(file_path, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Write column headers
            csvwriter.writerow(column_names)