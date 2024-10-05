import glob
import pandas as pd
import csv
import os
from datetime import datetime

class Platinum:
    @staticmethod
    def FinalData(year, month, day):
        path = 'F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/PollutionDataAnalysisProject'

        input_path = f"{path}/Gold_Hour/{year}/{month}/{day}"
        output_path = f"{path}/Platinum_Hour"
        isExist = os.path.exists(output_path)
        if not isExist:
            os.makedirs(output_path)

        csv_files = glob.glob(os.path.join(input_path, "*.csv"))

        if csv_files:
            desired_columns_order = ["State", "City", "Station", "Date", "CO", "NH3", "NO2", "OZONE", "PM10", "PM2.5", "SO2", "Checks", "AQI", "AQI_Quality","Longitude","Latitude"]

            for csv_file_path in csv_files:
                df = pd.read_csv(csv_file_path, header=0)[desired_columns_order]
                output_file_path = f"{output_path}/pollutiondata_Final.csv"
                df.to_csv(output_file_path, mode='a', index=False, header=False if os.path.exists(output_file_path) else True)

            print("All files processed successfully!")
        else:
            print("No CSV files found in the specified directory.")

# year = "2023"
# month = "11"
# day = "13"
# Platinum.FinalData(year, month, day)


