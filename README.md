# ANAC Open Data analyser

![PyPI - Python Version](https://img.shields.io/badge/python-3.12-3776AB?logo=python)

### > Directories

#### config
Configuration directory with ```config.yml```
- [ ] Define the starting/ending years to download ```YEAR_START_DOWNLOAD``` and ```YEAR_END_DOWNLOAD```
- [ ] Define the starting dataset to filter in ```ANAC_OD_FILE``` (it contains the ```bando_cig_*``` merged files from dynamic URLs)
- [ ] Define the starting/ending years to filter in ```YEAR_START_FILTER``` and ```YEAR_END_FILTER```
- [ ] Define the region to filter in ```ANAC_OD_REGION``` following ANAC region names
- [ ] Define the region output name ```ANAC_OD_REGION_OUT``` (convenience name for next use)

#### data
Directory with filtered data starting from the one downloaded.

#### download
Directory with download files from ANAC (zip and csv). 

#### pa_registry
Directory with PA registry (for PA type); source: [https://openbdap.rgs.mef.gov.it](https://openbdap.rgs.mef.gov.it)

#### stats
Directory with procurements stats.

#### utility_manager
Directory with utilities functions.

### > Files

#### ```01_anac_od_download.py```
Application to download public notices (tenders) from the ANAC website and create a global dataset.

#### ```02_anac_od_select.py```
Application to select ANAC Open Data of interest from the global dataset.

#### ```anac_regions.json```
List of regions according to ANAC nomenclature and filtering convenience name.

#### ```anac_dynamic_urls.json```
List of dynamic URLs (files) to download; source [https://dati.anticorruzione.it/opendata](https://dati.anticorruzione.it/opendata)

#### ```anac_static_urls.json```
List of static URLs (files) to download; source [https://dati.anticorruzione.it/opendata](https://dati.anticorruzione.it/opendata)

#### ``ssl_adapter.py``
Class to execute requests via HTTPS.