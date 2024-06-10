# 01_data_analyser.py

### IMPORT ###
import pandas as pd
from datetime import datetime
from pathlib import Path

### LOCAL IMPORT ###
from config import config_reader
from utility_manager.utilities import json_to_list_dict, json_to_sorted_dict, check_and_create_directory, list_files_by_type, get_values_from_dict_list, df_read_csv, df_print_details, script_info

### GLOBALS ###
yaml_config = config_reader.config_read_yaml("config.yml", "config")
# print(yaml_config) # debug
od_anac_dir = str(yaml_config["OD_ANAC_DIR"])
od_file_type = str(yaml_config["OD_FILE_TYPE"])
csv_sep = str(yaml_config["CSV_FILE_SEP"])
conf_file_cols_exc = str(yaml_config["CONF_COLS_EXCL_FILE"]) # JSON with columns to be excluded from reading
conf_file_cols_type = str(yaml_config["CONF_COLS_TYPE_FILE"]) # JSON with column types  
conf_file_stats_inc = str(yaml_config["CONF_COLS_STATS_FILE"]) # JSON with columns to be included in stats
stats_dir = str(yaml_config["STATS_DIR"])
tender_main_file = str(yaml_config["TENDER_MAIN_TABLE"])

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


def distinct_values_frequencies(df: pd.DataFrame, include_cols: list) -> pd.DataFrame:
    """
    Extracts the distinct values and their frequencies in percentage for each column of the given dataframe, excluding specified columns.
    
    Parameters:
        df (pd.DataFrame): The input dataframe.
        include_cols (list): A list of column names to be included int the analysis.
    
    Returns:
        pd.DataFrame: A dataframe containing the distinct values and their frequencies  in percentage for each column of the input dataframe, excluding  the specified columns.
    """
    # Use only the specified columns
    df_filtered = df[include_cols]
    # df_filtered = df.loc[:, include_cols]
    
    # Create an empty DataFrame to store the results
    result_df = pd.DataFrame(columns=['Column', 'Value', 'Frequency (%)'])
    
    # List to store the results
    result_list = []

    # Calculate the distinct values and their frequencies
    for col in df_filtered.columns:
        value_counts = df_filtered[col].value_counts(normalize=True) * 100
        value_counts = value_counts.round(2)  # Round frequencies to two decimal places
        for value, freq in value_counts.items():
            # result_df = pd.concat([result_df, pd.DataFrame({'Column': [col], 'Value': [value], 'Frequency (%)': [freq]})], ignore_index=True)
            result_list.append({'Column': col, 'Value': value, 'Frequency (%)': freq})
    
    # Create the result DataFrame from the list
    result_df = pd.DataFrame(result_list, columns=['Column', 'Value', 'Frequency (%)'])
    
    return result_df

def save_stats(df_stats:pd.DataFrame, file_name:str, stats_suffix:str, csv_sep:str = ";") -> None:
    """
    Saves a DataFrame containing statistical data to both CSV and Excel file formats.

    Parameters:
        df_stats (pd.DataFrame): The DataFrame containing the statistics to be saved.
        file_name (str): The base name for the output files (without extension). The function will append '{stats_suffix}' to the base name.
        stats_suffix (str): The suffix of the stats type.
        csv_sep (str): The CSV separator.

    Returns:
        None
    """
    stats_out_csv = Path(stats_dir) / f"{file_name}{stats_suffix}.csv"
    print("Writing CSV:", stats_out_csv)
    df_stats.to_csv(stats_out_csv, sep=csv_sep, index=False)
    stats_out_xlsx = Path(stats_dir) / f"{file_name}{stats_suffix}.xlsx"
    xls_sheet_name=f"{file_name.removesuffix("_csv")[0:31]}" # For compatibility with older versions of Excel
    print("Writing XLSX:", stats_out_xlsx)
    print("XLSX sheet name:", xls_sheet_name)
    df_stats.to_excel(stats_out_xlsx, sheet_name=f"{xls_sheet_name}", index=False)

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
    print("Directory:", od_anac_dir)
    list_od_files = list_files_by_type(od_anac_dir, od_file_type)
    list_od_files_len = len(list_od_files)
    print(f"Files '{od_file_type}' found: {list_od_files_len}")
    print()

    print(">> Reading the configuration file")
    
    print("File (columns excluded):", conf_file_cols_exc)
    list_col_exc_dic = json_to_list_dict(conf_file_cols_exc)
    # print(list_col_exc_dic) # debug

    print("File (columns type):", conf_file_cols_type)
    list_col_type_dic = json_to_sorted_dict(conf_file_cols_type)
    # print(list_col_type_dic) # debug
    
    print("File (stats columns):", conf_file_stats_inc)
    list_col_stats_dic = json_to_list_dict(conf_file_stats_inc)
    # print(list_col_stats_dic) # debug
    
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

        # Get the columns to be included in stats
        list_col_stats_inc = get_values_from_dict_list(list_col_stats_dic, file_od)
        list_col_stats_inc_len = len(list_col_stats_inc)
        
        # Read the file (dataset)
        df_od = df_read_csv(od_anac_dir, file_od, list_col_exc, list_col_type_dic, None, csv_sep)
        df_print_details(df_od, f"File '{file_od}'")
        print()

        # Add the column "cpv_division" that takes the first two characters of "cod_cpv" if it's not null
        if file_od == tender_main_file:
            print(f"> Updating main tender file '{file_od}'")
            df_od['cpv_division'] = df_od['cod_cpv'].apply(lambda x: x[:2] if pd.notnull(x) else None)
            df_od['accordo_quadro'] = df_od['cig_accordo_quadro'].apply(lambda x: 1 if pd.notna(x) else 0)

        # Stats 1 - Missing values
        print("> Creating stats")
        print("> Missing values")
        dic_od = summarize_dataframe_to_dict(df_od, file_od)
        # print(dic_od) # debug
        df_stats = summarize_dataframe_to_df(dic_od)
        # print(df_stats.head()) # debug
        print("> Saving stats")
        save_stats(df_stats, file_stem, "_stats_missing")
        print()

        # Stats 2 - Distinct values
        print("> Distinct values")
        print("Colums included for this stat:", list_col_stats_inc_len)
        print(list_col_stats_inc) # debug
        if list_col_stats_inc_len > 0:
            df_stats = distinct_values_frequencies(df_od, list_col_stats_inc)
            # print(df_stats.head()) # debug
            print("> Saving stats")
            save_stats(df_stats, file_stem, "_stats_distinct")
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