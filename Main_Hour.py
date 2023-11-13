import subprocess
from datetime import datetime
import os
# this file is use to run all the ETL pipeline
from SilverHour_DataCleansing import Silver
from GoldHour_DataTransformation import Gold
# from Platinum_FInalData import Platinum
# from Sql_DataTransfer import DataTransfer


year = str(datetime.now().year)
month = str(datetime.now().month)
day = str(datetime.now().day)

folder_path = f"F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/PollutionDataAnalysisProject/Bronze/{year}/{month}/{day}"  # Replace with the actual path

# Check if the folder doesn't exist
if not os.path.exists(folder_path):
    # continue 
    print("folder doesnt exist")
else:
    Silver.ProcessLastFileInDirectory(year,month,day)
    Gold.ProcessLastFileInDirectory(year,month,day)
    # Platinum.FinalData(year,month,day)
    # DataTransfer.DataTransferSQL()
    print("Hourly file data processed success")