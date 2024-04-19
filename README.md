# ETL_from_diff_sources

## DEDSCRIPTION

This script is an ETL pipeline that extracts data from different source files:
- CSV
- JSON
- XML

And put all data together on a single CSV file for further use or dump into a DB.
Also to generate a log file for the process

## WORKFLOW

1.- Extracts data from all CSV, JSON and XML files using "pandas" and "xml" APIs

2.- Transformed data to match format requirements(meters/kilograms instead inches/pounds)

3.- Loaded the transformed data into a single CSV file for stakeholders usage

4.- Generated a log file for the ETL process
