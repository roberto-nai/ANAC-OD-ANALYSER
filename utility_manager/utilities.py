import json
from pathlib import Path
import pandas as pd 

def json_to_list_dict(json_file: str) -> list:
    """
    Extracts and sorts key-value pairs from a JSON file alphabetically by the keys.

    Parameters:
        json_file (str): The path to the JSON file.

    Returns:
        list: A list of tuples containing tuples of key-value pairs extracted and sorted from the JSON file.
    """
    # Load JSON data from file
    with open(json_file, 'r') as file:
        data = json.load(file)
    
    # Extract key-value pairs into a list of dictionaries and sort alphabetically by keys
    sorted_key_value_pairs = [{key: value} for key, value in sorted(data.items())]
    
    return sorted_key_value_pairs

def json_to_sorted_dict(json_file: str) -> dict:
    """
    Extracts and sorts key-value pairs from a JSON file alphabetically by the keys.

    Parameters:
        json_file (str): The path to the JSON file.

    Returns:
        dict: A dictionary containing key-value pairs extracted and sorted from the JSON file.
    """
    # Load JSON data from file
    with open(json_file, 'r') as file:
        data = json.load(file)
    
    # Sort key-value pairs alphabetically by keys and return as a single dictionary
    sorted_key_value_pairs = {key: data[key] for key in sorted(data.keys())}
    
    return sorted_key_value_pairs

def check_and_create_directory(dir_name:str, dir_parent:str="") -> None:
    """
    Create a directory in its parent directory (optional).

    Parameters:
        dir_name (str): directory to be created.
        dir_parent (str): parent directory in which to create the directory.
    """

    path_directory = ""
    if dir_parent != "":
        path_directory = Path(dir_parent) / dir_name
    else:
        path_directory = Path(dir_name)
    if path_directory.exists() and path_directory.is_dir():
        print("The directory '{}' already exists: {}".format(dir_name, path_directory))
    else:
        path_directory.mkdir(parents=True, exist_ok=True)
        print("The directory '{}' has been created successfully: {}".format(dir_name, path_directory))
    print()


def list_files_by_type(directory:str, extension:str) -> list:
    """
    List files in the given directory with a specific extension, excluding macOS temporary files.
    
    Parameters:
        directory (str): The directory path to search in.
        extension (str): The file extension to filter by, including the leading dot (e.g., '.txt').

    Returns:
        A list of file names matching the extension and not being macOS temporary files.
    """
    # Create a Path object for the directory
    dir_path = Path(directory)
    file_list = []
    # List all files with the specified extension and filter out macOS temporary files
    file_list = [file.name for file in dir_path.glob(f'*{extension}') if not file.name.startswith('._')]
    return file_list


def get_values_from_dict_list(dict_list: list, key: str) -> list:
    """
    Given a list of dictionaries and a key, this function returns the list of values associated with the key.
    If the key is not found in any dictionary, an empty list is returned.

    Parameters:
        dict_list (list): List of dictionaries where each dictionary has a string key and a list of strings as values.
        key (str): Key to search for in the dictionaries.
    
    Returns
        List of values associated with the key or an empty list if the key is not found.
    """
    # Iterate over each dictionary in the list
    for dictionary in dict_list:
        # Check if the key exists in the dictionary
        if key in dictionary:
            # Return the value associated with the key
            return dictionary[key]
    # Return an empty list if the key is not found
    return []

def sql_create_database(db_name: str, drop_db: bool) -> str:
    """
    Generates an SQL script to create a database with the option to drop it if it already exists.

    Parameters:
    db_name (str): The name of the database to create.
    drop_db (bool): If True, adds the command to drop the database if it already exists.

    Returns:
    str: The generated SQL script.
    """
    # Initialise an empty list to hold the SQL commands
    sql_commands = []

    # If drop_db is True, add the DROP DATABASE command
    if drop_db:
        sql_commands.append(f"DROP DATABASE IF EXISTS {db_name};")

    # Add the CREATE DATABASE command
    sql_commands.append(f"CREATE DATABASE {db_name};")

    sql_commands.append(f"USE {db_name};\n")

    # Join the list of commands into a single string separated by newlines
    return "\n".join(sql_commands)

def df_to_sql_create_table_query(df: pd.DataFrame, drop_table: bool, primary_keys: list, table_name: str) -> str:
    """
    Generate a MySQL CREATE TABLE query from a pandas DataFrame and save it to a specified folder.
    
    Parameters:
        df (pd.DataFrame): The pandas DataFrame to convert to a SQL CREATE TABLE query.
        drop_table (bool): If True, add the DROP TABLE statement.
        primary_keys (list): List of primary keys.
        table_name (str): The name of the table to be created.

    Returns:
        str: A SQL query string for creating a table.
    """

    # Replace hyphens with underscores in column names and primary keys
    df.columns = [c.replace('-', '_') for c in df.columns]
    primary_keys = [key.replace('-', '_') for key in primary_keys]
    
    query = ""

    # Start building the query
    if drop_table:
        query = f"DROP TABLE IF EXISTS {table_name};\n"
        query += f"CREATE TABLE {table_name} (\n"
    else:
        query = f"CREATE TABLE {table_name} (\n"
    
    # Map pandas dtypes to MySQL types
    type_mapping = {
        'object': 'VARCHAR(255)',
        'int64': 'BIGINT',
        'float64': 'DOUBLE',
        'datetime64[ns]': 'DATETIME'
    }
    
    # Iterate through columns and their data types
    column_definitions = []
    for column, dtype in df.dtypes.items():
        mysql_dtype = type_mapping.get(str(dtype), 'VARCHAR(255)')  # Default to VARCHAR(255) if type is unknown
        # Add column and type to the definition list
        # column_definitions.append(f"  `{column}` {mysql_dtype}")
        if column in primary_keys:
            column_definitions.append(f"  `{column}` {mysql_dtype} NOT NULL")
        else:
            column_definitions.append(f"  `{column}` {mysql_dtype} NULL")
    
    # Join all column definitions into a single string
    query += ",\n".join(column_definitions)
    
    # Add primary key constraint if primary keys are provided
    if primary_keys:
        primary_keys_str = ", ".join(f"`{key}`" for key in primary_keys)
        query += f",\n  PRIMARY KEY ({primary_keys_str})"

    # Adde INDEX
    for key in primary_keys:
        query += f",\n  INDEX `{key}_idx` (`{key}`)"
    
    # Add the closing parenthesis and end the query
    query += '\n);'
    query += '\n'
    
    return query

def df_read_csv(dir_name: str, file_name: str, list_col_exc: list, list_col_type:dict, nrows:int, csv_sep: str = ";") -> pd.DataFrame:
    """
    Reads data from a CSV file into a pandas DataFrame excluding columns (if needed)

    Parameters:
        dir_name (str): the directory to the CSV file to be read.
        file_name (str): the filename to the CSV file to be read.
        list_col_exc (list): columns to be excluded.
        list_col_type (dict): columns type.
        nrows (int): rows to be read (if None, all).
        sep (str, optional): the delimiter string used in the CSV file. Defaults to ';'.

    Returns:
        pd.DataFrame: a pandas DataFrame containing the data read from the CSV file.
    """
    path_data = Path(dir_name) / file_name
    if nrows is not None:
        df = pd.read_csv(path_data, sep=csv_sep, dtype=list_col_type, nrows=nrows, low_memory=False)
    else:
        df = pd.read_csv(path_data, sep=csv_sep, dtype=list_col_type, low_memory=False)
    if len(list_col_exc) > 0:
        for col_name in list_col_exc:
                if col_name in df.columns:
                    del df[col_name]
    # df = df.drop_duplicates()
    return df


def df_print_details(df: pd.DataFrame, title: str) -> None:
    """
    Prints details of a pandas DataFrame, including its size and a preview of its contents.

    Parameters:
        df (pd.DataFrame): the DataFrame whose details are to be printed.
        title (str): a title for the printed output to describe the context of the DataFrame.

    Returns:
        None
    """

    #print(f"{title}")
    print(f"Dataframe size: {df.shape}\n")
    print(f"{title} dataframe preview:")
    print(df.head(), "\n\n")
    print(df.columns, "\n\n")

def sql_generate_foreign_keys(table_name: str, column_foreign_keys: list) -> str:
    """
    Generates SQL statements to set FOREIGN KEY constraints on the specified table.

    Parameters:
        table_name (str): The name of the table on which to set the FOREIGN KEY constraints.
        column_foreign_keys (list): A list of dictionaries containing columns and their respective foreign keys.
    Returns:
        str: A string containing the SQL statements to set the FOREIGN KEY constraints.
    """
    sql_statements = []
    
    for columns in column_foreign_keys:
        for column, foreign_key in columns.items():
            # Obtain the name of the table and the column of the foreign key
            foreign_table, foreign_column = foreign_key.split(".")
            # Create the SQL statement for the FOREIGN KEY constraint
            sql_statement = f"ALTER TABLE {table_name} ADD CONSTRAINT fk_{table_name}_{column} FOREIGN KEY ({column}) REFERENCES {foreign_table}({foreign_column});"
            sql_statements.append(sql_statement)
    
    sql_statement = "\n".join(sql_statements)
    sql_statement = sql_statement + "\n"
    return sql_statement

def script_info(file: str) -> tuple:
    """
    Returns the absolute path and the base name of the script file provided.

    Parameters:
        file (str): The file path to the script.

    Returns:
        tuple: A tuple containing the absolute path and the base name of the script.
    """
    
    script_path = Path(file).resolve()  # Converts the path to an absolute path
    script_name = script_path.name      # Gets the file name including extension

    return script_path, script_name