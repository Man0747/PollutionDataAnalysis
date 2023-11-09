import pandas as pd
import glob
import csv
import os
from datetime import datetime

class Silver:

    @staticmethod
    def DatacleansingForFile(input_file, output_path):
        df = pd.read_csv(input_file)

        # Define columns to clean
        columns_to_clean = ['pollutant_min', 'pollutant_max', 'pollutant_avg']

        # Clean 'NA' and blank columns and convert to numeric
        for column in columns_to_clean:
            if df[column].dtype != 'O':  # Check if the column is not of string data type
                df[column] = df[column].replace('NA', 0)

        df = df.dropna()
        df = df.drop('id', axis=1)
        df.rename(columns={"country": "Country", "state": "State", "city": "City", "station": "Station",
                          "last_update": "Date", "pollutant_id": "Pollutant_Type", "pollutant_avg": "Pollutant_Avg",
                          "pollutant_max": "Pollutant_Max"}, inplace=True)

        final_df = df.groupby(["Country", "State", "City", "Station", "Date", "Pollutant_Type"]).agg({"Pollutant_Avg": "mean", "Pollutant_Max": "max"}).reset_index()
        final_df["Pollutant_Avg"] = final_df["Pollutant_Avg"].round(2)
        final_df["Pollutant_Max"] = final_df["Pollutant_Max"].round(2)

        final_df["Pollutant_Data"] = final_df.apply(
            lambda row: row["Pollutant_Max"] if row["Pollutant_Type"] in ["OZONE1", "CO1"] else row["Pollutant_Avg"],
            axis=1)

        output_file_name = os.path.basename(input_file)
        output_file_path = os.path.join(output_path, f'Silver_{output_file_name}')
        final_df.to_csv(output_file_path, index=False)

    @staticmethod
    def ProcessLastFileInDirectory(input_path, output_path):
        csv_files = glob.glob(input_path + "/*.csv")

        if csv_files:
            # Sort the files by modification time to get the most recent one
            csv_files.sort(key=os.path.getmtime)
            last_file = csv_files[-1]
            Silver.DatacleansingForFile(last_file, output_path)
        else:
            print("No CSV files found in the input path.")

# Usage
path = 'F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/PollutionDataAnalysisProject'
input_path = path + '/Bronze'

year = str(datetime.now().year)
month = str(datetime.now().month)
day = str(datetime.now().day)

input_directory = input_path + "/" + year + "/" + month + "/" + day
output_directory = os.path.join(path, 'Silver_Hour', year, month, day)
isExist = os.path.exists(output_directory)
if not isExist:
    os.makedirs(output_directory)

Silver.ProcessLastFileInDirectory(input_directory, output_directory)
