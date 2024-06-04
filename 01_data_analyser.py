# 01_data_analyser.py

### IMPORT ###
import pandas as pd
from datetime import datetime
from pathlib import Path

### LOCAL IMPORT ###
from config import config_reader
from utility_manager.utilities import json_to_list_dict, check_and_create_directory, list_files_by_type, get_values_from_dict_list, df_read_csv, df_print_details, script_info

### GLOBALS ###
yaml_config = config_reader.config_read_yaml("config.yml", "config")
# print(yaml_config) # debug
od_dir = str(yaml_config["OD_DIR"])
od_file_type = str(yaml_config["OD_FILE_TYPE"])
csv_sep = str(yaml_config["OD_FILE_SEP"])
conf_file_cols_exc = str(yaml_config["CONF_COLS_EXCL_FILE"]) # JSON
conf_file_primary_keys = str(yaml_config["CONF_PRIMARY_KEYS_FILE"]) # JSON
conf_file_foreign_keys = str(yaml_config["CONF_FOREIGN_KEYS_FILE"]) # JSON
stats_dir = str(yaml_config["STATS_DIR"])

script_path, script_name = script_info(__file__)

### FUNCTIONS ###

def summarize_dataframe_to_dict(df: pd.DataFrame, file_name: str) -> dict:
    """
    Creates a dictionary summarizing the input DataFrame with the file name, and the count of missing (empty) values for each column.

    Parameters:
        df (pd.DataFrame): The DataFrame to summarize.
        file_name (str): The name of the file associated with the DataFrame.

    Returns:
        dict: a dictionary containing the file name and missing value counts for each column.
    """
    # Count the number of missing values in each column of the DataFrame
    missing_counts = df.isnull().sum()
    # Convert the Series to a dictionary
    missing_counts_dict = missing_counts.to_dict()
    # Count the number of duplicate rows, considering all columns
    duplicate_rows_count = df.duplicated().sum()
    # Get the number of rows and columns in the DataFrame
    num_rows, num_columns = df.shape
    # Calculate the ratio of duplicate rows to total rows
    ratio_dup = duplicate_rows_count / num_rows if num_rows > 0 else 0  # Avoid division by zero

    # Create the summary dictionary
    summary_dict = {
        'file_name': file_name,
        'rows_num':num_rows,
        'cols_num':num_columns,
        'missing_values': missing_counts_dict,
        'duplicated_rows': duplicate_rows_count,
        'duplicated_rows_perc': round(ratio_dup,2)
    }
    return summary_dict

def summarize_dataframe_to_df(summary_dict:dict) -> pd.DataFrame:
    """
    Saves the given summary dictionary to a CSV file, where each key-value pair in the dictionary becomes a column. The 'Missing Values Per Column' nested dictionary is expanded into separate columns.

    Parameters:
        summary_dict (dict): The summary dictionary to save.
        csv_file_name (str): The file name for the CSV file.

    Returns:
        pd.DataFrame: A dataframe with data.
    """
    # Flatten the 'Missing Values Per Column' dictionary into the main dictionary with prefix
    for key, value in summary_dict['missing_values'].items():
        summary_dict[f'Missing_{key}'] = value
    # Remove the original nested dictionary key
    del summary_dict['missing_values']
    
    # Convert the dictionary to a DataFrame
    df = pd.DataFrame([summary_dict])
    # Save the DataFrame to a CSV file
    # df.to_csv(csv_file_name, index=False)
    return df

### MAIN ###
def main():
    print()
    print(f"*** PROGRAM START ({script_name}) ***")
    print()

    start_time = datetime.now().replace(microsecond=0)
    print("Start process: " + str(start_time))
    print()

    print(">> Preparing output directories")
    check_and_create_directory(stats_dir)
    print()

    print(">> Scanning Open Data catalogue")
    print("Directory:", od_dir)
    list_od_files = list_files_by_type(od_dir, od_file_type)
    list_od_files_len = len(list_od_files)
    print(f"Files '{od_file_type}' found: {list_od_files_len}")
    print()

    print(">> Reading the configuration file")
    print("File (columns excluded):", conf_file_cols_exc)
    list_col_exc_dic = json_to_list_dict(conf_file_cols_exc)

    # print(list_col_exc_dic) # debug
    list_col_exc_dic_len = len(list_col_exc_dic)
    print("Files indexed (columns excluded):", list_col_exc_dic_len)
    print()

    print(">> Analysing Open Data files")
    print()
    for file_od in list_od_files:
        # File info
        print("> Reading file")
        print("File:", file_od)
        file_path = Path(file_od)
        file_stem = file_path.stem # get the name without extension

        # Get the columns excluded from the configuration list
        list_col_exc = get_values_from_dict_list(list_col_exc_dic, file_od)
        list_col_exc_len = len(list_col_exc)
        print("Columns exluded from the dataframe:", list_col_exc_len)
        
        # Read the file (dataset)
        df_od = df_read_csv(od_dir, file_od, list_col_exc, None, csv_sep)
        df_print_details(df_od, f"File '{file_od}'")
        print()

        # Stats
        print("> Creating stats")
        dic_od = summarize_dataframe_to_dict(df_od, file_od)
        # print(dic_od) # debug
        df_stats = summarize_dataframe_to_df(dic_od)
        print(df_stats.head())
        print()

        # Save the stats
        print("> Saving stats")
        stats_out_csv = Path(stats_dir) / f"{file_stem}_stats.csv"
        print("Writing:", stats_out_csv)
        df_stats.to_csv(stats_out_csv, sep=csv_sep, index=False)
        stats_out_xlsx = Path(stats_dir) / f"{file_stem}_stats.xlsx"
        xls_sheet_name=f"{file_stem.removesuffix("_csv")[0:30]}" # For compatibility with older versions of Excel
        print("Writing:", stats_out_xlsx)
        print("Sheet name:", xls_sheet_name)
        df_stats.to_excel(stats_out_xlsx, sheet_name=f"{xls_sheet_name}", index=False)
        print()
        print("-"*3)
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