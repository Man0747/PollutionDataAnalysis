import glob
import numpy as np
import pandas as pd
import csv
import requests
import os
from datetime import datetime
import mysql.connector


class Gold:

    @staticmethod
    def DataTransformationForFile(input_file, output_directory):
        
        df = pd.read_csv(input_file)

        final_df = df.pivot_table(
            index=["State", "City", "Station", "Date","Longitude","Latitude"],
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

        final_df["AQI"] = round(final_df[["PM2.5", "PM10", "SO2", "NO2", "NH3", "CO", "OZONE"]].max(axis=1))
        final_df.loc[final_df["PM2.5"] + final_df["PM10"] <= 0, "AQI"] = np.NaN
        final_df.loc[final_df.Checks < 3, "AQI"] = np.NaN

        final_df["AQI_Quality"] = final_df["AQI"].apply(lambda x: get_AQI_bucket(x))

        final_df = final_df.dropna(subset=['AQI'])
        # DataTransfer.DataTransferSQL(final_df)
        # DataTransfer.DataHouronlyTransferSQL(final_df)
        
        output_file_name = os.path.basename(input_file)
        output_file_path = os.path.join(output_directory, f'Gold_{output_file_name}.csv')
        final_df.to_csv(output_file_path, index=False)

        tracking_file_path = "F:\Education\COLLEGE\PROGRAMING\Python\PROJECTS\PollutionDataAnalysisProject\Gold_processed_files.txt"
        with open(tracking_file_path, 'a') as tracking_file:
            tracking_file.write(output_file_name + '\n')
    @staticmethod
    def CleanProcessedFilesForNextDay(output_directory):
        # Clean the tracking file for the next day
        tracking_file_path = "F:\Education\COLLEGE\PROGRAMING\Python\PROJECTS\PollutionDataAnalysisProject\Gold_processed_files.txt"

        if os.path.exists(tracking_file_path):
            os.remove(tracking_file_path)

    @staticmethod
    def ProcessLastFileInDirectory(year, month, day):
        path = 'F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/PollutionDataAnalysisProject'

        input_directory = path + '/Silver_Hour' + "/" + year + "/" + month + "/" + day
        output_directory = path + '/Gold_Hour' + "/" + year + "/" + month + "/" + day
        is_exist = os.path.exists(output_directory)
        if not is_exist:
            os.makedirs(output_directory)

        # Clean processed files for the next day
        # Gold.CleanProcessedFilesForNextDay(output_directory)

        # Load the list of processed files
        tracking_file_path = "F:\Education\COLLEGE\PROGRAMING\Python\PROJECTS\PollutionDataAnalysisProject\Gold_processed_files.txt"
        processed_files = set()
        if os.path.exists(tracking_file_path):
            with open(tracking_file_path, 'r') as tracking_file:
                processed_files = set(tracking_file.read().splitlines())

        csv_files = glob.glob(input_directory + "/*.csv")

        if csv_files:
            for file in csv_files:
                if os.path.basename(file) not in processed_files:
                    Gold.DataTransformationForFile(file, output_directory)
        else:
            print("No CSV files found in the input directory.")

# Usage
# Specify the year, month, and day for the directory you want to process
# year = '2023'
# month = '11'
# day = '13'

# Gold.ProcessLastFileInDirectory(year, month, day)
# Usage




# Gold.ProcessLastFileInDirectory(input_directory, output_directory)
