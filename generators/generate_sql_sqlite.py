import json

def generate(schema):
    # Create mappings from UUIDs to names
    column_type_map = {ct['uuid']: ct['name'] for ct in schema['column_types']}
    relationship_type_map = {rt['uuid']: rt['name'] for rt in schema['relationship_types']}
    property_type_map = {pt['uuid']: pt['name'] for pt in schema['property_types']}

    # Function to get SQL data type from column type name
    def get_sql_type(column_type_name):
        type_mapping = {
            'VARCHAR(255)': 'TEXT',
            'INT': 'INTEGER',
            'FLOAT': 'REAL',
            'BOOLEAN': 'INTEGER',  # SQLite uses INTEGER for booleans (0 or 1)
            'DATE': 'TEXT',        # SQLite stores dates as TEXT or INTEGER
            'DATETIME': 'TEXT',
            'BLOB': 'BLOB',
        }
        return type_mapping.get(column_type_name, 'TEXT')

    # Collect SQL statements
    sql_statements = []

    # Keep track of intermediary tables for Array types
    intermediary_tables = []

    # Enable foreign key support
    sql_statements.append('PRAGMA foreign_keys = ON;\n')

    # Iterate over each table
    for table in schema['tables']:
        table_name = table['name']
        columns_sql = []
        primary_keys = []
        foreign_keys = []
        
        # Iterate over columns
        for column in table['columns']:
            column_name = column['name']
            column_type_uuid = column['type']
            column_type_name = column_type_map.get(column_type_uuid)
            
            # Special handling for Array(VARCHAR(255))
            if column_type_name == 'Array(VARCHAR(255))':
                # Create intermediary table instead of column
                if column['relationship'] and len(column['relationship']) > 0:
                    relationship = column['relationship'][0]
                    intermediary_table_name = relationship['name']
                    
                    related_table_uuid = relationship['table_uuid']
                    related_column_uuid = relationship['column_uuid']
                    
                    # Get the related table name and column name
                    related_table = next((t for t in schema['tables'] if t['uuid'] == related_table_uuid), None)
                    if related_table:
                        related_table_name = related_table['name']
                        related_column = next((col for col in related_table['columns'] if col['uuid'] == related_column_uuid), None)
                        if related_column:
                            related_column_name = related_column['name']
                        else:
                            related_column_name = 'UUID'  # Default to 'UUID' if not found
                    else:
                        related_table_name = None
                        related_column_name = 'UUID'
                else:
                    # No relationship info
                    intermediary_table_name = f"{table_name}_{column_name}"
                    related_table_name = None
                    related_column_name = column_name

                intermediary_tables.append({
                    'name': intermediary_table_name,
                    'column_name': column_name,
                    'primary_table': table_name,
                    'primary_column': 'UUID',  # Assuming 'UUID' is the primary key
                    'foreign_table': related_table_name,
                    'foreign_column': related_column_name
                })
                continue  # Skip adding this column to the main table
            else:
                sql_type = get_sql_type(column_type_name)
            
            column_def = f'"{column_name}" {sql_type}'
            
            # Handle properties (e.g., nullable, primary key)
            if column['properties']:
                for prop in column['properties']:
                    prop_name = property_type_map.get(prop['type'])
                    prop_value = prop['value']
                    
                    if prop_name == 'nullable' and not prop_value:
                        column_def += ' NOT NULL'
                    if prop_name == 'PrimaryKey' and prop_value == 'true':
                        primary_keys.append(column_name)
            
            columns_sql.append(column_def)
            
            # Handle relationships for non-array types (if needed)
            if column['relationship']:
                for rel in column['relationship']:
                    related_table_uuid = rel['table_uuid']
                    related_column_uuid = rel['column_uuid']
                    related_table_name = next((t['name'] for t in schema['tables'] if t['uuid'] == related_table_uuid), None)
                    related_column_name = next((col['name'] for t in schema['tables'] if t['uuid'] == related_table_uuid for col in t['columns'] if col['uuid'] == related_column_uuid), None)
                    if related_table_name and related_column_name:
                        foreign_keys.append(f'    FOREIGN KEY ("{column_name}") REFERENCES "{related_table_name}"("{related_column_name}")')
        
        # Create the CREATE TABLE statement
        create_table_sql = f'CREATE TABLE "{table_name}" (\n    ' + ',\n    '.join(columns_sql)
        
        # Add primary key constraint
        if primary_keys:
            pk = ", ".join([f'"{pk}"' for pk in primary_keys])
            create_table_sql += f',\n    PRIMARY KEY ({pk})'
        
        # Add foreign key constraints
        if foreign_keys:
            create_table_sql += ',\n' + ',\n'.join(foreign_keys)
        
        # Close the CREATE TABLE statement
        create_table_sql += '\n);\n'
        
        sql_statements.append(create_table_sql)

    # Create intermediary tables for Array types
    for inter_table in intermediary_tables:
        table_name = inter_table['name']
        primary_table = inter_table['primary_table']
        primary_column = inter_table['primary_column']
        foreign_table = inter_table['foreign_table']
        foreign_column = inter_table['foreign_column']
        column_name = inter_table['column_name']
        columns = []
        constraints = []

        # Primary table foreign key
        columns.append(f'"{primary_table}_ID" TEXT NOT NULL')
        constraints.append(f'    FOREIGN KEY ("{primary_table}_ID") REFERENCES "{primary_table}"("{primary_column}")')

        if foreign_table:
            # Foreign table foreign key
            columns.append(f'"{column_name}" TEXT NOT NULL')
            constraints.append(f'    FOREIGN KEY ("{column_name}") REFERENCES "{foreign_table}"("{foreign_column}")')
        else:
            # If no foreign table, include the column as TEXT
            columns.append(f'"{foreign_column}" TEXT NOT NULL')

        # Create the CREATE TABLE statement
        create_table_sql = f'CREATE TABLE "{table_name}" (\n    ' + ',\n    '.join(columns)
        
        if constraints:
            create_table_sql += ',\n' + ',\n'.join(constraints)
        
        create_table_sql += '\n);\n'
        
        sql_statements.append(create_table_sql)

    # Write the SQL statements to a file
    with open('create_database_sqlite.sql', 'w') as f:
        for stmt in sql_statements:
            f.write(stmt + '\n')
