import sys
import json
from generators import generate_sql_mysql, generate_sql_postgres, generate_sql_sqlite
from config import HELP_TEXT

def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py <schema_file_path> [generator_type]")
        return

    schema_path = sys.argv[1]

    try:
        with open(schema_path, 'r') as file:
            schema = json.load(file)
        print(f"Successfully loaded schema from '{schema_path}'")
    except FileNotFoundError:
        print(f"Error: File '{schema_path}' not found.")
        return
    except json.JSONDecodeError:
        print(f"Error: '{schema_path}' is not a valid JSON file.")
        return

    if len(sys.argv) == 3:
        try:
            option = int(sys.argv[2])
            run_generator(option, schema)
        except ValueError:
            print("Error: Generator type must be an integer.")
    else:
        interactive_mode(schema)

def run_generator(option, schema):
    generators = {
        1: ("MySQL", generate_sql_mysql.generate),
        2: ("PostgreSQL", generate_sql_postgres.generate),
        3: ("SQLite", generate_sql_sqlite.generate),
        5: ("JSON", None)
    }

    if option in generators:
        name, generator = generators[option]
        if generator:
            print(f"Generating {name} SQL...")
            generator(schema)
            print(f"{name} SQL generation complete.")
        else:
            print(f"{name} file generation not implemented yet.")
    else:
        print(f"Invalid option: {option}")

def interactive_mode(schema):
    while True:
        print("\n" + HELP_TEXT)
        user_input = input("Enter an option number (or 'q' to quit): ").strip().lower()
        
        if user_input == 'q':
            print("Exiting the program. Goodbye!")
            break
        
        try:
            option = int(user_input)
            run_generator(option, schema)
        except ValueError:
            print("Invalid input. Please enter a number or 'q' to quit.")

if __name__ == "__main__":
    main()
