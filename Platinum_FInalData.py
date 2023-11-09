import glob
import pandas as pd
import csv
import os
from datetime import datetime

class Platinum:
    def FinalData(year):
        path = 'F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/PollutionDataAnalysisProject'

        input_path = path + '/Gold'
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
# Assuming your columns are in a specific order
        df_list = [pd.read_csv(file, header=0) for file in csv_files]
        desired_columns_order = ["State", "City", "Station", "Date", "CO", "NH3", "NO2", "OZONE", "PM10", "PM2.5", "SO2", "Checks", "AQI", "AQI_Quality"]
        combined_df = pd.concat(df_list, ignore_index=True)
        print(combined_df.columns)
        final_df = combined_df[desired_columns_order]

        output_file_path = output_path + f'/pollutiondata_Final - Copy.csv'
        final_df.to_csv(output_file_path, mode='a', index=False, header=False if os.path.exists(output_file_path) else True)



  # Append without headers

y = 2023
year = str(y)
Platinum.FinalData(year)
