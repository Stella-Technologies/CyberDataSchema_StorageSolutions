import json

def generate(schema):
    data = {}

    # Iterate over each table in the schema
    for table in schema['tables']:
        table_name = table['name']
        # Initialize each table with an single dictionary
        data[table_name] = [
            {column['name']: None for column in table['columns']}
        ]

    # Write the sample JSON file
    with open('sample_data.json', 'w') as f:
        json.dump(data, f, indent=4)


