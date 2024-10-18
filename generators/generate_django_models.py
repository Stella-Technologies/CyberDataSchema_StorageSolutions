import json

def generate(schema):

    # Create mappings from UUIDs to names
    column_type_map = {ct['uuid']: ct['name'] for ct in schema['column_types']}
    relationship_type_map = {rt['uuid']: rt['name'] for rt in schema['relationship_types']}
    property_type_map = {pt['uuid']: pt['name'] for pt in schema['property_types']}

    # Function to get Django field type and default parameters from column type name
    def get_django_field_type(column_type_name):
        type_mapping = {
            'VARCHAR(255)': ('models.CharField', 'max_length=255'),
            'INT': ('models.IntegerField', ''),
            'FLOAT': ('models.FloatField', ''),
            'BOOLEAN': ('models.BooleanField', ''),
            'DATE': ('models.DateField', ''),
            'DATETIME': ('models.DateTimeField', ''),
            'BLOB': ('models.BinaryField', ''),
            'UUID': ('models.UUIDField', ''),
            # Add more types as needed
        }
        return type_mapping.get(column_type_name, ('models.CharField', 'max_length=255'))

    # Function to get Django relationship field
    def get_django_relationship_field(relationship_type_name, related_model_name, field_options):
        if relationship_type_name == 'OneToOne':
            return f'models.OneToOneField("{related_model_name}", on_delete=models.CASCADE{field_options})'
        elif relationship_type_name == 'ManyToMany':
            return f'models.ManyToManyField("{related_model_name}"{field_options})'
        else:
            # Default to ForeignKey
            return f'models.ForeignKey("{related_model_name}", on_delete=models.CASCADE{field_options})'

    # Collect model class definitions
    model_classes = []

    # Keep track of intermediary models for Array types
    intermediary_models = []

    # Keep track of imports needed
    imports = set()
    imports.add('from django.db import models')

    # Iterate over each table
    for table in schema['tables']:
        table_name = table['name']
        class_name = table_name  # Use the table name as is

        # Start building the class definition
        class_def = f'class {class_name}(models.Model):\n'

        # Fields definitions
        fields_def = []

        # Keep track of primary key fields
        primary_keys = []

        # Iterate over columns
        for column in table['columns']:
            column_name = column['name']
            column_type_uuid = column['type']
            column_type_name = column_type_map.get(column_type_uuid)

            # Get the field type and default parameters
            django_field_class, django_field_params = get_django_field_type(column_type_name)

            field_options = []

            # Handle properties (e.g., null, blank, primary_key)
            if column.get('properties'):
                for prop in column['properties']:
                    prop_name = property_type_map.get(prop['type'])
                    prop_value = prop['value']
                    if prop_name == 'nullable':
                        field_options.append(f'null={str(prop_value)}')
                    if prop_name == 'blank':
                        field_options.append(f'blank={str(prop_value)}')
                    if prop_name == 'PrimaryKey':
                        if prop_value == 'true' or prop_value == True:
                            field_options.append('primary_key=True')
                            primary_keys.append(column_name)

            # Handle relationships
            if column.get('relationship'):
                for rel in column['relationship']:
                    related_table_uuid = rel['table_uuid']
                    relationship_type_uuid = rel.get('relationship_type_uuid')
                    relationship_type_name = relationship_type_map.get(relationship_type_uuid, 'ForeignKey')

                    related_table = next((t for t in schema['tables'] if t['uuid'] == related_table_uuid), None)
                    if related_table:
                        related_table_name = related_table['name']
                        field_options_str = ''
                        if field_options:
                            field_options_str = ', ' + ', '.join(field_options)
                        django_field = get_django_relationship_field(relationship_type_name, related_table_name, field_options_str)
                        field_def = f'    {column_name} = {django_field}'
                        fields_def.append(field_def)
                        break  # Assume only one relationship per column
            else:
                # No relationship, add the field normally
                field_params = []
                if django_field_params:
                    field_params.append(django_field_params)
                if field_options:
                    field_params.extend(field_options)
                field_params_str = ', '.join(field_params)
                field_def = f'    {column_name} = {django_field_class}({field_params_str})'
                fields_def.append(field_def)

        # If no fields defined, add pass
        if not fields_def:
            fields_def.append('    pass')

        # Combine the class definition
        class_def += '\n'.join(fields_def) + '\n'
        model_classes.append(class_def)

    # Handle intermediary models (if any)
    model_classes.extend(intermediary_models)

    # Write the models.py file
    with open('models.py', 'w') as f:
        # Write imports
        for imp in sorted(imports):
            f.write(imp + '\n')
        f.write('\n')

        # Write model classes
        for model_class in model_classes:
            f.write(model_class + '\n')
