# 01_data_analyser.py

### IMPORT ###
import pandas as pd
from datetime import datetime
from pathlib import Path
import glob

### LOCAL IMPORT ###
from config import config_reader
from utility_manager.utilities import json_to_list_dict, json_to_sorted_dict, check_and_create_directory, list_files_by_type, get_values_from_dict_list, df_to_sql_create_table_query, df_read_csv, sql_create_database, sql_generate_foreign_keys, script_info

### GLOBALS ###
yaml_config = config_reader.config_read_yaml("config.yml", "config")
# print(yaml_config) # debug

csv_sep = str(yaml_config["CSV_FILE_SEP"])

od_file_type = str(yaml_config["OD_FILE_TYPE"])

# ANAC data
od_anac_dir = str(yaml_config["OD_ANAC_DIR"])

# ISTAT data
od_istat_dir = str(yaml_config["OD_ISTAT_DIR"])
dic_columns_fix = dict(yaml_config["OD_ISTAT_COLUMNS_FIX"]) 

# BDAP data
od_bdap_dir = str(yaml_config["OD_BDAP_DIR"])

conf_file_cols_exc = str(yaml_config["CONF_COLS_EXCL_FILE"]) # JSON
conf_file_cols_type = str(yaml_config["CONF_COLS_TYPE_FILE"]) # JSON
conf_file_primary_keys = str(yaml_config["CONF_PRIMARY_KEYS_FILE"]) # JSON
conf_file_foreign_keys = str(yaml_config["CONF_FOREIGN_KEYS_FILE"]) # JSON
conf_file_table_names = str(yaml_config["CONF_TABLES_ENG"]) # JSON
sql_dir_tables = str(yaml_config["SQL_DIR_TABLES"])
sql_drop_table = bool(yaml_config["SQL_DROP_TABLE"])
sql_file_type = str(yaml_config["SQL_FILE_TYPE"])
db_name_drop = bool(yaml_config["SQL_DROP_DB"])

# OUTPUT
sql_dir_db = str(yaml_config["SQL_DIR_DB"]) # output
db_name = str(yaml_config["SQL_DB_NAME"]) # output

script_path, script_name = script_info(__file__)

### FUNCTIONS ###

def process_files_to_sql(od_dir: str, list_od_files: list, list_col_exc_dic: list, list_col_type_dic:list, dict_rename_col:dict, sql_drop_table: bool, list_primary_key_dic:list, sql_dir_tables:str, csv_sep: str = ";") -> None:
    """
    Processes a list of files, excluding specified columns, and creates SQL table files.

    Parameters:
        od_dir (str): Directory containing the original data files.
        list_od_files (list): List of file names to be processed.
        list_col_exc_dic (list): List of dictionaries with columns to be excluded for each file.
        list_col_type_dic (list): List of dictionaries specifying the type of each column.
        dict_rename_col (dict): Dictionary with column to be renamed.
        sql_drop_table (bool): Flag indicating whether to include a DROP TABLE statement in the SQL.
        list_primary_key_dic (list): List of dictionaries with primary key columns for each file.
        sql_dir_tables (str): Directory where the generated SQL files will be saved.
        csv_sep (str): Separator used in the CSV files. Default is ';'.

    Returns:
        None
    """

    for file_od in list_od_files:
        # File info
        print("> Reading file")
        print("File:", file_od)
        file_path = Path(file_od)
        file_stem = file_path.stem # get the name without extension

        # Get the columns excluded from the configuration list
        list_col_exc = get_values_from_dict_list(list_col_exc_dic, file_od)
        list_col_exc_len = len(list_col_exc)
        print("Columns excluded from the dataframe:", list_col_exc_len)
        
        # Read the file (dataset)
        df_od = df_read_csv(od_dir, file_od, list_col_exc, list_col_type_dic, 1, csv_sep) # read just one row, no needed for SQL
        print()

        # Checks whether each key is a column present in the DataFrame (therefore to be renamed)
        if dict_rename_col is not None:
            for key in dict_rename_col:
                if key in df_od.columns:
                    df_od.rename(columns={key: dict_rename_col[key]}, inplace=True)

        # Create the SQL
        print("> Creating SQL - table file")
        table_name = file_stem.removesuffix("_csv")
        table_name_clean = table_name.replace("-","_")
        sql_db_file = f"{table_name_clean}.sql"
        # print(list_col_key_dic) # debug
        list_p_key = get_values_from_dict_list(list_primary_key_dic, file_od) # get the key list by file name 
        print("Table name / file name:", table_name_clean, "/", sql_db_file)
        print("Table primary keys:", list_p_key)
        sql = df_to_sql_create_table_query(df_od, sql_drop_table, list_p_key, table_name_clean)
        sql_path = Path(sql_dir_tables) / sql_db_file
        print("Writing:", sql_path)
        with open(sql_path, "w") as fp:
            fp.write(sql)
        print("-"*3)
    print()


### MAIN ###
def main():
    print()
    print(f"*** PROGRAM START ({script_name}) ***")
    print()

    start_time = datetime.now().replace(microsecond=0)
    print("Start process: " + str(start_time))
    print()

    print(">> Preparing output directories")
    check_and_create_directory(sql_dir_tables)
    check_and_create_directory(sql_dir_db)
    print()

    # ANAC OD
    print(">> Scanning Open Data catalogue")
    print("Directory:", od_anac_dir)
    list_od_files = list_files_by_type(od_anac_dir, od_file_type)
    list_od_files_len = len(list_od_files)
    print(f"Files '{od_file_type}' found: {list_od_files_len}")
    print()

    # ISTAT
    print(">> Scanning ISTAT catalogue")
    print("Directory:", od_istat_dir)
    list_istat_files = list_files_by_type(od_istat_dir, od_file_type)
    list_istat_files_len = len(list_istat_files)
    print(f"Files '{od_file_type}' found: {list_istat_files_len}")
    print()

    print(">> Scanning BDAP catalogue")
    print("Directory:", od_bdap_dir)
    list_bdap_files = list_files_by_type(od_bdap_dir, od_file_type)
    list_bdap_files_len = len(list_bdap_files)
    print(f"Files '{od_file_type}' found: {list_bdap_files_len}")
    print()

    print(">> Reading the configuration file")
    print("File (columns excluded):", conf_file_cols_exc)
    print("File (columns keys):", conf_file_primary_keys)
    list_col_exc_dic = json_to_list_dict(conf_file_cols_exc)
    list_primary_key_dic = json_to_list_dict(conf_file_primary_keys)
    list_foreign_key_dic = json_to_list_dict(conf_file_foreign_keys)
    list_col_type_dic = json_to_sorted_dict(conf_file_cols_type)
    list_tables_end_dic = json_to_sorted_dict(conf_file_table_names)

    # print(list_col_exc_dic) # debug
    # print(list_col_type_dic) # debug

    # print(list_col_exc_dic) # debug
    list_col_exc_dic_len = len(list_col_exc_dic)
    list_col_key_dic_len = len(list_primary_key_dic)
    print("Files indexed (columns excluded):", list_col_exc_dic_len)
    print("Files indexed (columns keys):", list_col_key_dic_len)
    print()

    print(">> Creating SQL files")
    process_files_to_sql(od_anac_dir, list_od_files, list_col_exc_dic, list_col_type_dic, None, sql_drop_table, list_primary_key_dic, sql_dir_tables, csv_sep)
    process_files_to_sql(od_istat_dir, list_istat_files, list_col_exc_dic, list_col_type_dic, dic_columns_fix, sql_drop_table, list_primary_key_dic, sql_dir_tables, csv_sep)
    process_files_to_sql(od_bdap_dir, list_bdap_files, list_col_exc_dic, list_col_type_dic, dic_columns_fix, sql_drop_table, list_primary_key_dic, sql_dir_tables, csv_sep)
    print()
    
    # Create the final SQL 
    # 0) Create the file
    print(">> Creating final SQL file")
    sql_db_file = f"_{db_name}.sql"
    path_out = Path(sql_dir_db) / sql_db_file
    print("Directory output:", sql_dir_db)
    print("File output:", path_out)
    print()

    # 1) Get all SQL files of tables
    print(f"> Listing '{sql_file_type}' files in '{sql_dir_tables}'")
    list_sql_files = list_files_by_type(sql_dir_tables, sql_file_type)
    list_sql_files_len = len(list_sql_files)
    print(f"Files '{sql_file_type}' found: {list_sql_files_len}")
    print()

    # 2) Add DB creation statement
    print("> Creating DB statement")
    print("DB name:", db_name)
    sql = sql_create_database(db_name, db_name_drop)
    sql_path = Path(sql_dir_tables) / sql_db_file
    print("Writing:", sql_path)
    with open(sql_path, "w") as fp:
        fp.write(sql)
    print()

    # 3) Join SQL files from tables 
    print("> Creating unique SQL file")
    read_files = glob.glob(f"{sql_dir_tables}/*.sql")
    with open(path_out, "wb") as outfile:
        for f in read_files:
            with open(f, "rb") as infile:
                outfile.write(infile.read())
    print("[OK]")
    print()
    
    # 4) Add Foreign keys
    list_fk = []
    print("> Adding FK statement")
    for sql_file_name in list_sql_files:
        table_name = Path(sql_file_name).stem
        print("Table name:", table_name)
        list_f_key = get_values_from_dict_list(list_foreign_key_dic, table_name)
        print("Foreign keys:", list_f_key)
        if len(list_f_key) > 0:
            str_fk = sql_generate_foreign_keys(table_name, list_f_key)
            list_fk.append(str_fk)
    # print(list_fk) # debug
    path_out = Path(sql_dir_db) / sql_db_file
    with open(path_out, "a") as fp:
        for sql_string in list_fk:
            fp.write(sql_string)
    
    # 5) Add tables in english
    print(list_tables_end_dic)
    rename_statements = [f"RENAME TABLE {old_name} TO {new_name.upper()};" for old_name, new_name in list_tables_end_dic.items()]
    with open(path_out, "a") as fp:
        for rename_s in rename_statements:
            fp.write(rename_s + "\n")

    print()
    print("Final database SQL file to be imported in MySQL in:", path_out)
    print()

    # Program end
    end_time = datetime.now().replace(microsecond=0)
    delta_time = end_time - start_time

    print()
    print("End process:", end_time)
    print("Time to finish:", delta_time)
    print()

    print()
    print("*** PROGRAM END ***")
    print()


if __name__ == "__main__":
    main()