import json

def generate(schema):
    data = {}

    # Iterate over each table in the schema
    for table in schema['tables']:
        table_name = table['name']
        # Initialize each table with an empty list
        data[table_name] = []

    # Write the clean JSON file
    with open('clean_data.json', 'w') as f:
        json.dump(data, f, indent=4)

