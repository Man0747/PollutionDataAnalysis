import pandas as pd
import glob
import csv
# import requests
import os
from datetime import datetime

class Silver:

    def Datacleansing(year,month,day):

        
        path = 'F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/PollutionDataAnalysisProject'
        input_path = path + '/Bronze'
        input_path = os.path.join(input_path, year, month, day)
        output_path = os.path.join(path, 'Silver', year, month, day)

        isExist = os.path.exists(output_path)
        if not isExist:
            os.makedirs(output_path)

        csv_files = glob.glob(input_path + "/*.csv")
        if not csv_files:
            print("No CSV files found in the specified directory.")
            return

        df_list = []

        for file in csv_files:
            try:
                df = pd.read_csv(file)
                allowed_columns = ['country', 'state', 'city', 'station', 'last_update', 'pollutant_id', 'pollutant_min', 'pollutant_max', 'pollutant_avg','longitude','latitude']
                current_columns = df.columns.tolist()
                if len(current_columns) > len(allowed_columns):
                    print(f"Number of columns ({len(current_columns)}) exceeds the allowed limit ({len(allowed_columns)}). Deleting file: {file}")
                    # Delete the file
                    os.remove(file)
                    print(f"File deleted: {file}")
                elif len(current_columns) < len(allowed_columns):
                    print(f"Number of columns ({len(current_columns)}) decreeds the allowed limit ({len(allowed_columns)}). Deleting file: {file}")
                    # Delete the file
                    os.remove(file)
                    print(f"File deleted: {file}")
                else:
                    print(f"Number of columns ({len(current_columns)}) is within the allowed limit ({len(allowed_columns)}). No action required.")
                    # Extract the date from the 'last_update' column in the file
                    file_date = pd.to_datetime(df['last_update'], format="%d-%m-%Y %H:%M:%S").dt.date.iloc[0]
                    if file_date == datetime(int(year), int(month), int(day)).date():
                        df_list.append(df)
                    else:
                        print(f"Skipping file: {file} as the date doesn't match.")
                        # Delete the file with a mismatched date
                        os.remove(file)
                        print(f"File '{file}' has been deleted.")
            except pd.errors.ParserError as e:
                print(f"ParserError occurred for file: {file}")
                print(f"Error message: {e}")
                # Delete the problematic file
                os.remove(file)
                print(f"File '{file}' has been deleted.")

        if not df_list:
            print("No valid CSV files found after error handling.")
            return

        
        combined_df = pd.concat(df_list, ignore_index=True)
        
        print(combined_df)

        # Define columns to clean
        columns_to_clean = ['pollutant_min', 'pollutant_max', 'pollutant_avg']

        # Clean 'NA' and blank columns and convert to numeric
        for column in columns_to_clean:
            if combined_df[column].dtype != 'O':  # Check if the column is not of string data type
                combined_df[column] = combined_df[column].replace('NA', 0)
               
        combined_df["Date"] = pd.to_datetime(combined_df["last_update"], format="%d-%m-%Y %H:%M:%S").dt.date
        combined_df.rename(columns={"country": "Country", "state": "State","city": "City","station": "Station","pollutant_id": "Pollutant_Type","pollutant_avg": "Pollutant_Avg","pollutant_max": "Pollutant_Max","longitude": "Longitude","latitude": "Latitude"}, inplace=True)

        final_df = combined_df.groupby(["Country", "State", "City", "Station", "Date", "Pollutant_Type","Longitude","Latitude"]).agg({"Pollutant_Avg": "mean", "Pollutant_Max": "max"}).reset_index()
        final_df["Pollutant_Avg"] = final_df["Pollutant_Avg"].round(2)
        final_df["Pollutant_Max"] = final_df["Pollutant_Max"].round(2)

        final_df["Pollutant_Data"] = final_df.apply(lambda row: row["Pollutant_Max"] if row["Pollutant_Type"] in ["OZONE1", "CO1"] else row["Pollutant_Avg"], axis=1)

        current_datetime = datetime.now().strftime('%Y%m%d')
        output_file_path = output_path + f'/Silver_pollutiondata_{current_datetime}.csv'
        final_df.to_csv(output_file_path, index=False)

# Silver.Datacleansing('2024','1','30')
