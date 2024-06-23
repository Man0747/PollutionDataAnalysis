import subprocess
from datetime import datetime,timedelta
import os
import sys


# print(sys.path)
# print(sys.executable)
# try:
#     import mysql.connector
# except ImportError:
#     print("mysql-connector module not found. Installing...")
    # subprocess.run(["pip", "install", "mysql.connector"])
# this file is use to run all the ETL pipeline
from Silver_DataCleansing import Silver
from Gold_DataTransformation import Gold
from Platinum_FInalData import Platinum
# from Sql_HeavyLoadDataTransfer import DataTransfer
from Sql_IncrementalLoadDataTransfer import ImplementLoadDataTransfer
# Function to read the last executed date from the file
def read_last_executed_date():
    file_path = "F:\Education\COLLEGE\PROGRAMING\Python\PROJECTS\PollutionDataAnalysisProject\DailyDataAddLogs.txt"
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            date_str = file.read().strip()
            return datetime.strptime(date_str, "%Y-%m-%d")
    else:
        # If the file doesn't exist, return a default date
        return datetime(2000, 1, 1)

# Function to write the last executed date to the file
def write_last_executed_date(date):
    file_path = "F:\Education\COLLEGE\PROGRAMING\Python\PROJECTS\PollutionDataAnalysisProject\DailyDataAddLogs.txt"

    with open(file_path, 'w') as file:
        file.write(date.strftime("%Y-%m-%d"))

# AUTO RUN
today = datetime.now()
last_executed_date = read_last_executed_date()

# Execute the pipeline for each day from the last executed date to yesterday
try:
    for current_date in (last_executed_date + timedelta(days=n) for n in range(1, (today - last_executed_date).days)):
        year = str(current_date.year)
        month = str(current_date.month)
        day = str(current_date.day)

        folder_path = f"F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/PollutionDataAnalysisProject/Bronze/{year}/{month}/{day}"

        # Check if the folder doesn't exist
        if not os.path.exists(folder_path):
            print(f"Folder doesn't exist for {year}-{month}-{day}")
        else:
            Silver.Datacleansing(year, month, day)
            Gold.DataTransformation(year, month, day)
            Platinum.FinalData(year, month, day)
            ImplementLoadDataTransfer.DataTransferSQL(year,month,day)
            print(f"Data processed successfully for {year}-{month}-{day}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Ensure DataTransferSQL and write_last_executed_date are executed
    # DataTransfer.DataTransferSQL()
    # write_last_executed_date(today - timedelta(days=1))
    write_last_executed_date(current_date)

# Update the last executed date in the file