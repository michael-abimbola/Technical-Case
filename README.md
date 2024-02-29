# Technical-Case KommatiPara Client Data Processing
This repository contains a Pyspark script for processing client data from CSV files.
The focus was to filter the data based on the country name, renaming the columns to 
a more readable format and dropping columns that contained personal information. The
data manipulation tasks listed are performed on two data files in CSV format. 
- Dataset one contains the clients customers personal information 
- Dataset two contains the customers credit card information

# Getting Started
1. Clone the repository
git clone https://github.com/michael-abimbola/Technical-Case.git

2. Navigate to the project directory
cd Technical-Case

3. Use the package manager pip to install the packages in requirement.txt 
pip install -r requirements.txt

# Usage  
To use the solution input the path to both datasets and the country name needed to be filtered on 
The following code under the usage comment in source_code/app.py should be updated with the respective values
Final_data = client_data_creation(<dataset_two.csv>, <dataset_one.csv>, <country_name>)

To receive the final output while on the project directory run 
python source_code/app.py

# Features
- Filtering by Country: Filters the dataset based on a specified country name.
- Renaming Columns: Renames specified columns in the dataset.
- Dropping Personal Information: Drops specified personal information columns from the dataset.
- Logging: Includes logging functionality for debugging and tracking the processing steps.

# Output
The processed data is saved to a CSV file named final_data.csv in the client_data directory. 

# Logging
Logs are stored in a rotating file handler, with a new log file created daily. 
The log files are named according to the date (e.g., 29-02-2024.log) and limited to a maximum size of 2048 bytes.