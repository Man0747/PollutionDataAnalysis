import glob
import numpy as np
import pandas as pd
import csv
import requests
import os
from datetime import datetime

class Gold:
    @staticmethod
    def DataTransformationForFile(input_file, output_directory):
        df = pd.read_csv(input_file)

        final_df = df.pivot_table(
            index=["State", "City", "Station", "Date"],
            columns='Pollutant_Type',
            values='Pollutant_Data',
            fill_value=0
        ).reset_index()

        def get_AQI_bucket(x):
            if x <= 50:
                return "Good"
            elif x <= 100:
                return "Satisfactory"
            elif x <= 200:
                return "Moderate"
            elif x <= 300:
                return "Poor"
            elif x <= 400:
                return "Very Poor"
            elif x > 400:
                return "Severe"
            else:
                return np.NaN

        final_df["Checks"] = (final_df["PM2.5"] > 0).astype(int) + \
                            (final_df["PM10"] > 0).astype(int) + \
                            (final_df["SO2"] > 0).astype(int) + \
                            (final_df["NO2"] > 0).astype(int) + \
                            (final_df["NH3"] > 0).astype(int) + \
                            (final_df["CO"] > 0).astype(int) + \
                            (final_df["OZONE"] > 0).astype(int)

        final_df["AQI"] = round(final_df[["PM2.5", "PM10", "SO2", "NO2","NH3", "CO", "OZONE"]].max(axis = 1))
        final_df.loc[final_df["PM2.5"] + final_df["PM10"] <= 0, "AQI"] = np.NaN
        final_df.loc[final_df.Checks < 3, "AQI"] = np.NaN

        final_df["AQI_Quality"] = final_df["AQI"].apply(lambda x: get_AQI_bucket(x))

        final_df = final_df.dropna(subset=['AQI'])

        output_file_name = os.path.basename(input_file)
        output_file_path = os.path.join(output_directory, f'Gold_{output_file_name}.csv')
        final_df.to_csv(output_file_path, index=False)

    @staticmethod
    def ProcessLastFileInDirectory(input_directory, output_directory):
        csv_files = glob.glob(input_directory + "/*.csv")

        if csv_files:
            # Sort the files by modification time to get the most recent one
            csv_files.sort(key=os.path.getmtime)
            last_file = csv_files[-1]
            Gold.DataTransformationForFile(last_file, output_directory)
        else:
            print("No CSV files found in the input directory.")

# Usage
path = 'F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/PollutionDataAnalysisProject'
year = str(datetime.now().year)
month = str(datetime.now().month)
day = str(datetime.now().day)
input_directory = path + '/Silver_Hour' + "/" + year + "/" + month + "/" + day
output_directory = path + '/Gold__Hour' + "/" + year + "/" + month + "/" + day
isExist = os.path.exists(output_directory)
if not isExist:
    os.makedirs(output_directory)

Gold.ProcessLastFileInDirectory(input_directory, output_directory)
