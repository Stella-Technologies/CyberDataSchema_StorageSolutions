import json
import xlsxwriter

def generate(schema):
    # Create a new Excel file and add a workbook
    workbook = xlsxwriter.Workbook('output.xlsx')

    # Iterate over each table in the schema
    for table in schema['tables']:
        table_name = table['name']
        worksheet = workbook.add_worksheet(table_name)

        # Extract column names
        column_names = [column['name'] for column in table['columns']]

        # Write column headers
        for col_num, column_name in enumerate(column_names):
            worksheet.write(0, col_num, column_name)

    # Close the workbook
    workbook.close()

