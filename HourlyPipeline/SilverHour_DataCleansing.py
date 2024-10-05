import pandas as pd
import glob
import os

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
        # df = df.drop('id', axis=1)
        df.rename(columns={"country": "Country", "state": "State", "city": "City", "station": "Station",
                           "last_update": "Date", "pollutant_id": "Pollutant_Type", "pollutant_avg": "Pollutant_Avg","pollutant_max": "Pollutant_Max","longitude": "Longitude","latitude": "Latitude"}, inplace=True)

        final_df = df.groupby(["Country", "State", "City", "Station", "Date", "Pollutant_Type","Longitude","Latitude"]).agg({"Pollutant_Avg": "mean", "Pollutant_Max": "max"}).reset_index()
        final_df["Pollutant_Avg"] = final_df["Pollutant_Avg"].round(2)
        final_df["Pollutant_Max"] = final_df["Pollutant_Max"].round(2)

        final_df["Pollutant_Data"] = final_df.apply(
            lambda row: row["Pollutant_Max"] if row["Pollutant_Type"] in ["OZONE1", "CO1"] else row["Pollutant_Avg"],
            axis=1)

        output_file_name = os.path.basename(input_file)
        output_file_path = os.path.join(output_path, f'Silver_{output_file_name}')
        final_df.to_csv(output_file_path, index=False)

        # Save the processed file name to a tracking file
        tracking_file_path = "F:\Education\COLLEGE\PROGRAMING\Python\PROJECTS\PollutionDataAnalysisProject\Silver_processed_files.txt"
        with open(tracking_file_path, 'a') as tracking_file:
            tracking_file.write(output_file_name + '\n')

    @staticmethod
    def CleanProcessedFilesForNextDay(output_path):
        # Clean the tracking file for the next day
        tracking_file_path = 'F:\Education\COLLEGE\PROGRAMING\Python\PROJECTS\PollutionDataAnalysisProject\Silver_processed_files.txt'
        if os.path.exists(tracking_file_path):
            os.remove(tracking_file_path)

    @staticmethod
    def ProcessLastFileInDirectory(year, month, day):
        path = 'F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/PollutionDataAnalysisProject'
        input_path = os.path.join(path, 'Bronze', year, month, day)
        output_path = os.path.join(path, 'Silver_Hour', year, month, day)
        is_exist = os.path.exists(output_path)
        if not is_exist:
            os.makedirs(output_path)

        # Clean processed files for the next day
        # Silver.CleanProcessedFilesForNextDay(output_path)

        # Load the list of processed files
        tracking_file_path = 'F:\Education\COLLEGE\PROGRAMING\Python\PROJECTS\PollutionDataAnalysisProject\Silver_processed_files.txt'
        processed_files = set()
        if os.path.exists(tracking_file_path):
            with open(tracking_file_path, 'r') as tracking_file:
                processed_files = set(tracking_file.read().splitlines())

        csv_files = glob.glob(input_path + "/*.csv")

        if csv_files:
            for file in csv_files:
                if os.path.basename(file) not in processed_files:
                    Silver.DatacleansingForFile(file, output_path)
        else:
            print("No CSV files found in the input path.")

# Usage
# Specify the year, month, and day for the directory you want to process
# year = '2023'
# month = '11'
# day = '13'

# Silver.ProcessLastFileInDirectory(year, month, day)


