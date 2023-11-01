import glob
import pandas as pd
import csv
# import requests
import os
from datetime import datetime

class Platinum:
    def FinalData(year):
        path = 'F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/PollutionDataAnalysisProject'

        input_path = path + '/Gold'
        #AUTO RUN
        # year = str(datetime.now().year)
        # month = str(datetime.now().month)
        # day = str(datetime.now().day - 1)

        #MANUAL RUN
        # input_year = int(input("Enter the Year : "))
        # input_month = int(input("Enter the Month : "))
        # input_day = int(input("Enter the Day : "))
        # year = str(input_year)
        # month = str(input_month)
        # day = str(input_day)



        # input_path = input_path + "/" + year + "/" + month + "/" + day
        input_path = input_path + "/" + year  
        output_path = path + '/Platinum'
        isExist = os.path.exists(output_path)
        if not isExist:
            os.makedirs(output_path)

        csv_files = []

        
        for root, _, files in os.walk(input_path):
            for file in files:
                if file.endswith(".csv"):
                    csv_files.append(os.path.join(root, file))

        df_list = (pd.read_csv(file) for file in csv_files)

        combined_df = pd.concat(df_list, ignore_index=True)
        final_df = combined_df

        output_file_path = output_path + f'/pollutiondata_Final.csv'
        final_df.to_csv(output_file_path, mode='a', index=False)
        # final_df.to_csv(output_file_path, index=False)

y=2020
year = str(y)
Platinum.FinalData(year)
