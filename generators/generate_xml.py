import json
import os
import xml.etree.ElementTree as ET
from xml.dom import minidom

def generate(schema):
    # Create a directory to store XML files
    output_dir = 'xml_output'
    os.makedirs(output_dir, exist_ok=True)

    # Iterate over each table in the schema
    for table in schema['tables']:
        table_name = table['name']
        columns = table['columns']

        # Create the root element for the table
        root = ET.Element(table_name)

        # Since there's no data, we'll just create a template with column names
        # Add a single empty row element with column sub-elements
        row_element = ET.SubElement(root, 'Row')

        for column in columns:
            column_name = column['name']
            column_element = ET.SubElement(row_element, column_name)
            # You can add attributes or text to column_element if needed

        # Create an ElementTree object
        tree = ET.ElementTree(root)

        # Generate a pretty-printed XML string
        xml_str = minidom.parseString(ET.tostring(root)).toprettyxml(indent="   ")

        # Write the XML string to a file
        file_path = os.path.join(output_dir, f'{table_name}.xml')
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(xml_str)
