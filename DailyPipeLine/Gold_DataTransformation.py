import glob
import numpy as np
import pandas as pd
import csv
import requests
import os
from datetime import datetime

class Gold:
    def DataTransformation(year,month,day) :
        path ='F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/PollutionDataAnalysisProject'

        input_path = path + '/Silver'
        
        # AUTO RUN
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

        input_path = input_path + "/" + year + "/" + month + "/" + day

        output_path = path + '/Gold' + "/" + year + "/" + month + "/" + day
        isExist = os.path.exists(output_path)
        if not isExist:
            os.makedirs(output_path)


        csv_files = glob.glob(input_path + "/*.csv")
        if not csv_files:
            print("No CSV files found in the specified directory.")
            return

        df_list = (pd.read_csv(file) for file in csv_files)

        combined_df = pd.concat(df_list, ignore_index=True)

        final_df = combined_df.pivot_table( 
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
        print(final_df.columns)
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
        # columns_to_drop = ["PM2.5_AQI", "PM10_AQI", "SO2_AQI", "NO2_AQI","NH3_AQI", "CO_AQI", "OZONE_AQI","Checks"]
        # final_df.drop(columns_to_drop, axis=1, inplace=True)

        current_datetime = datetime.now().strftime('%Y%m%d')
        output_file_path = output_path + f'/Gold_pollutiondata_{current_datetime}.csv'
        final_df.to_csv(output_file_path, index=False)

# Gold.DataTransformation('2024','5','19')