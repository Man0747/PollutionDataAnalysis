import glob
import numpy as np
import pandas as pd
import csv
import requests
import os
from datetime import datetime
import mysql.connector


class DataTransfer:
    def DataTransferSQL(final_df):
    # MySQL Database Configuration
        db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': 'Impetus@123',
            'database': 'pollutiondata'
        }

        # Connect to MySQL
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Read the CSV file into a pandas data frame
        final_df.rename(columns={"Date": "Pol_Date", "PM2.5": "PM25"}, inplace=True)
        final_df['Pol_Date'] = pd.to_datetime(final_df['Pol_Date'], format='%d-%m-%Y %H:%M:%S', errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

        table_name = "pollutiondata.onlylatesthourdata"

        data_delete = f"DELETE FROM {table_name};"
        cursor.execute(data_delete)
        connection.commit()
        # Explicitly specify column names in the query
        columns_str = ", ".join(final_df.columns)
        values_str = ", ".join(["%s" for _ in final_df.columns])
        insert_query = f'INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})'

        # Insert data from the data frame into MySQL
        for _, row in final_df.iterrows():
            print("Insert Query:", insert_query)
            print("Row Values:", tuple(row))
            cursor.execute(insert_query, tuple(row))

        # Commit changes and close the connection
        connection.commit()
        connection.close()

    def DataHouronlyTransferSQL(final_df):
    # MySQL Database Configuration
        db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': 'Impetus@123',
            'database': 'pollutiondata'
        }

        # Connect to MySQL
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Read the CSV file into a pandas data frame
        final_df.rename(columns={"Date": "Pol_Date", "PM2.5": "PM25"}, inplace=True)
        final_df['Pol_Date'] = pd.to_datetime(final_df['Pol_Date'], format='%d-%m-%Y %H:%M:%S', errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

        table_name = "pollutiondata.hourlydata"

        # Explicitly specify column names in the query
        columns_str = ", ".join(final_df.columns)
        values_str = ", ".join(["%s" for _ in final_df.columns])
        insert_query = f'INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})'

        # Insert data from the data frame into MySQL
        for _, row in final_df.iterrows():
            print("Insert Query:", insert_query)
            print("Row Values:", tuple(row))
            cursor.execute(insert_query, tuple(row))

        # Commit changes and close the connection
        connection.commit()
        connection.close()

    
        

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
        DataTransfer.DataTransferSQL(final_df)
        DataTransfer.DataHouronlyTransferSQL(final_df)
        output_file_name = os.path.basename(input_file)
        output_file_path = os.path.join(output_directory, f'Gold_{output_file_name}.csv')
        final_df.to_csv(output_file_path, index=False)

    @staticmethod
    def ProcessLastFileInDirectory(year,month,day):
        path = 'F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/PollutionDataAnalysisProject'

        input_file = path + '/Silver_Hour' + "/" + year + "/" + month + "/" + day
        output_directory = path + '/Gold__Hour' + "/" + year + "/" + month + "/" + day
        isExist = os.path.exists(output_directory)
        if not isExist:
            os.makedirs(output_directory)

        csv_files = glob.glob(input_file + "/*.csv")

        if csv_files:
            # Sort the files by modification time to get the most recent one
            csv_files.sort(key=os.path.getmtime)
            last_file = csv_files[-1]
            Gold.DataTransformationForFile(last_file, output_directory)
        else:
            print("No CSV files found in the input directory.")

# Usage




# Gold.ProcessLastFileInDirectory(input_directory, output_directory)
