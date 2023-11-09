import subprocess
from datetime import datetime
import os
# this file is use to run all the ETL pipeline
from Silver_DataCleansing import Silver
from Gold_DataTransformation import Gold
from Platinum_FInalData import Platinum

# AUTO RUN
year = str(datetime.now().year)
month = str(datetime.now().month)
day = str(datetime.now().day)

# # Demo RUN
# year = str(datetime.now().year)
# month = str(datetime.now().month)
# day = str(datetime.now().day)


# CODE FOR MANUAL RUN
# input_year = int(input("Enter the Year : "))
# input_year = 2023
# input_month = 11
# input_day = 8
# year = str(input_year)
# month = str(input_month)
# day = str(input_day)
# for i in range(1, 32):
    # input_day = i
    # day = str(input_day)
folder_path = f"F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/PollutionDataAnalysisProject/Bronze/{year}/{month}/{day}"  # Replace with the actual path

# Check if the folder doesn't exist
if not os.path.exists(folder_path):
    # continue 
    print("folder doesnt exist")
else:
    Silver.Datacleansing(year,month,day)
    Gold.DataTransformation(year,month,day)
    Platinum.FinalData(year,month,day)

