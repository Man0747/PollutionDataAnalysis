import mysql.connector
from datetime import datetime, timedelta
import os

# Import your other modules
from Silver_DataCleansing import Silver
from Gold_DataTransformation import Gold
from Platinum_FInalData import Platinum
from Sql_HourlyDataCleaning import DataCleaning
from Sql_IncrementalLoadDataTransfer import ImplementLoadDataTransfer
from Ml_DataTransfer import ML_SqlDataTransfer

# Database configurations
local_db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'admin',
    'database': 'udyaansaathidata'
}

azure_db_config = {
    'host': 'mysqlmannan01.mysql.database.azure.com',
    'user': 'mannan',
    'password': 'Khetan@123',
    'database': 'udyaansaathidata',
    'client_flags': [mysql.connector.ClientFlag.SSL],
    'ssl_ca': 'F:/Education/COLLEGE/PROGRAMING/Python/Codes/PolutionDataAnalysis/Devlopment/PollutionDataAnalysis/DigiCertGlobalRootG2.crt.pem'
}

def get_db_connection(config):
    return mysql.connector.connect(**config)

def read_last_executed_date(config):
    connection = get_db_connection(config)
    cursor = connection.cursor()
    cursor.execute("SELECT last_executed_date FROM LastExecuted ORDER BY id DESC LIMIT 1")
    result = cursor.fetchone()
    connection.close()
    if result:
        return result[0]
    else:
        # If there's no record, return a default date
        return datetime(2000, 1, 1).date()

def write_last_executed_date(config, date):
    connection = get_db_connection(config)
    cursor = connection.cursor()
    cursor.execute("INSERT INTO LastExecuted (last_executed_date) VALUES (%s)", (date,))
    connection.commit()
    connection.close()

def process_data_for_date(year, month, day, config):
    folder_path = f"F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/PollutionDataAnalysisProject/Bronze/{year}/{month}/{day}"

    if not os.path.exists(folder_path):
        print(f"Folder doesn't exist for {year}-{month}-{day}")
    else:
        # print(f"Config value: {config}")  # Add this line to check the value of config
        if config == azure_db_config:
            print("Skipping data processing for Azure configuration.")
            ImplementLoadDataTransfer.DataTransferSQL(year, month, day, config)
            DataCleaning.DataTransferSQL(year, month, day, config)
            ML_SqlDataTransfer.InsertMlDataInSql(config)
            print(f"Data update in azure sql successfully for {year}-{month}-{day}")
        else:
            Silver.Datacleansing(year, month, day)
            Gold.DataTransformation(year, month, day)
            Platinum.FinalData(year, month, day)
            ImplementLoadDataTransfer.DataTransferSQL(year, month, day, config)
            DataCleaning.DataTransferSQL(year, month, day, config)
            ML_SqlDataTransfer.InsertMlDataInSql(config)
            print(f"Data processed successfully for {year}-{month}-{day}")

# Function to run the pipeline for a specific configuration
def run_pipeline_for_config(config):
    today = datetime.now().date()
    last_executed_date = read_last_executed_date(config)
    current_date = last_executed_date
    try:
        for current_date in (last_executed_date + timedelta(days=n) for n in range(1, (today - last_executed_date).days)):
            year = str(current_date.year)
            month = str(current_date.month)
            day = str(current_date.day)
            process_data_for_date(year, month, day, config)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Update the last executed date in the specified database
        write_last_executed_date(config, current_date)

# Run the pipeline separately for local and Azure databases
run_pipeline_for_config(local_db_config)
run_pipeline_for_config(azure_db_config)

