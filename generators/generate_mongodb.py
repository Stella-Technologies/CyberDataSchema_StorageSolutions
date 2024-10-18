import json
import os

def generate(schema):
    # Create a directory to store schema files
    output_dir = 'mongodb_schemas'
    os.makedirs(output_dir, exist_ok=True)

    # Mapping from custom types to MongoDB/BSON types
    type_mapping = {
        'VARCHAR(255)': 'string',
        'INT': 'int',
        'FLOAT': 'double',
        'BOOLEAN': 'bool',
        'DATE': 'date',
        'DATETIME': 'date',
        'BLOB': 'binData',
        'UUID': 'string',
        # Add more types as needed
    }

    # Iterate over each table in the schema
    for table in schema['tables']:
        collection_name = table['name']
        columns = table['columns']

        # Build the JSON schema for the collection
        schema_dict = {
            'bsonType': 'object',
            'title': collection_name,
            'properties': {},
            'required': [],
            'additionalProperties': False
        }

        # Initialize the required fields as a set to avoid duplicates
        required_fields = set()

        for column in columns:
            column_name = column['name']
            column_type_uuid = column['type']
            column_type_name = next((ct['name'] for ct in schema['column_types'] if ct['uuid'] == column_type_uuid), 'string')
            mongodb_type = type_mapping.get(column_type_name, 'string')

            # Default property schema
            property_schema = {
                'bsonType': mongodb_type
            }

            # Handle properties (e.g., nullable, required)
            is_nullable = True  # Default to nullable
            if column.get('properties'):
                for prop in column['properties']:
                    prop_type_uuid = prop['type']
                    prop_type_name = next((pt['name'] for pt in schema['property_types'] if pt['uuid'] == prop_type_uuid), None)
                    prop_value = prop['value']

                    if prop_type_name == 'nullable':
                        is_nullable = prop_value
                    if prop_type_name == 'PrimaryKey' and prop_value == True:
                        required_fields.add(column_name)

            if not is_nullable:
                required_fields.add(column_name)

            # Add the property schema to the collection schema
            schema_dict['properties'][column_name] = property_schema

        # Assign the list of required fields without duplicates
        schema_dict['required'] = list(required_fields)

        # Write the collection schema to a JSON file
        file_path = os.path.join(output_dir, f'{collection_name}_schema.json')
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump({'$jsonSchema': schema_dict}, f, indent=4)

