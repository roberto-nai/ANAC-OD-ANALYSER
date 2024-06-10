# ANAC Open Data analyser

![PyPI - Python Version](https://img.shields.io/badge/python-3.12-3776AB?logo=python)

### > Directories

#### config
Configuration directory with ```config.yml```.  

#### open_data
Directory with downloaded ANAC Open Data Catalogue (see this project: [https://github.com/roberto-nai/ANAC-OD-DOWNLOADER](https://github.com/roberto-nai/ANAC-OD-DOWNLOADER)).  
Open Data are also available on Zenodo: [https://doi.org/10.5281/zenodo.11452793](https://doi.org/10.5281/zenodo.11452793).  

#### sql_db
Directory with SQL file with all table definitions (merged from ```sql_tables```).   

#### sql_tables
Directory with SQL file with single table definition.   

#### stats
Directory with procurements stats.

#### utility_manager
Directory with utilities functions.

### > Script Execution

#### ```01_data_analyser.py```
Application to analyse the dataset.

#### ```02_data_sql.py```
Application create a database script in ```SQL_DIR_DB``` following the JSON configuration files for PK, FK, column types and table names in English. At the end of the process, the SQL file in ```SQL_DIR_DB``` contains the complete database structure.  

#### ```conf_cols_excluded.json```
List of columns (features) to be ignored.

#### ```conf_cols_foreign_keys.json```
List of columns (features) to be used as foreign keys.

#### ```conf_cols_keys.json```
List of columns (features) to be used as primary keys.  

### > Script Dependencies
See ```requirements.txt``` for the required libraries (```pip install -r requirements.txt```).  
