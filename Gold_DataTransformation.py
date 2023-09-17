import glob
import numpy as np
import pandas as pd
import csv
import requests
import os
from datetime import datetime

class Gold:
    def DataTransformation() :
        path ='F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/PollutionDataAnalysisProject'

        input_path = path + '/Silver'
        year = str(datetime.now().year)
        month = str(datetime.now().month)
        day = str(datetime.now().day - 1)
        input_path = input_path + "/" + year + "/" + month + "/" + day

        output_path = path + '/Gold' + "/" + year + "/" + month + "/" + day
        isExist = os.path.exists(output_path)
        if not isExist:
            os.makedirs(output_path)


        csv_files = glob.glob(input_path + "/*.csv")

        df_list = (pd.read_csv(file) for file in csv_files)

        combined_df = pd.concat(df_list, ignore_index=True)

        final_df = combined_df.pivot_table( 
                                index=["State", "City", "Station", "Date"],
                                columns='Pollutant_Type', 
                                values='Pollutant_Data',
                                fill_value=0
                                ).reset_index()

        def get_PM25_subindex(x):
            if x <= 30:
                return x * 50 / 30
            elif x <= 60:
                return 50 + (x - 30) * 50 / 30
            elif x <= 90:
                return 100 + (x - 60) * 100 / 30
            elif x <= 120:
                return 200 + (x - 90) * 100 / 30
            elif x <= 250:
                return 300 + (x - 120) * 100 / 130
            elif x > 250:
                return 400 + (x - 250) * 100 / 130
            else:
                return 0

        final_df["PM2.5_AQI"] = final_df["PM2.5"].apply(lambda x: get_PM25_subindex(x)).round(2)

        def get_PM10_subindex(x):
            if x <= 50:
                return x
            elif x <= 100:
                return x
            elif x <= 250:
                return 100 + (x - 100) * 100 / 150
            elif x <= 350:
                return 200 + (x - 250)
            elif x <= 430:
                return 300 + (x - 350) * 100 / 80
            elif x > 430:
                return 400 + (x - 430) * 100 / 80
            else:
                return 0

        final_df["PM10_AQI"] = final_df["PM10"].apply(lambda x: get_PM10_subindex(x)).round(2)


        def get_SO2_subindex(x):
            if x <= 40:
                return x * 50 / 40
            elif x <= 80:
                return 50 + (x - 40) * 50 / 40
            elif x <= 380:
                return 100 + (x - 80) * 100 / 300
            elif x <= 800:
                return 200 + (x - 380) * 100 / 420
            elif x <= 1600:
                return 300 + (x - 800) * 100 / 800
            elif x > 1600:
                return 400 + (x - 1600) * 100 / 800
            else:
                return 0

        final_df["SO2_AQI"] = final_df["SO2"].apply(lambda x: get_SO2_subindex(x)).round(2)

        def get_NO2_subindex(x):
            if x <= 40:
                return x * 50 / 40
            elif x <= 80:
                return 50 + (x - 40) * 50 / 40
            elif x <= 180:
                return 100 + (x - 80) * 100 / 100
            elif x <= 280:
                return 200 + (x - 180) * 100 / 100
            elif x <= 400:
                return 300 + (x - 280) * 100 / 120
            elif x > 400:
                return 400 + (x - 400) * 100 / 120
            else:
                return 0

        final_df["NO2_AQI"] = final_df["NO2"].apply(lambda x: get_NO2_subindex(x)).round(2)

        def get_NH3_subindex(x):
            if x <= 200:
                return x * 50 / 200
            elif x <= 400:
                return 50 + (x - 200) * 50 / 200
            elif x <= 800:
                return 100 + (x - 400) * 100 / 400
            elif x <= 1200:
                return 200 + (x - 800) * 100 / 400
            elif x <= 1800:
                return 300 + (x - 1200) * 100 / 600
            elif x > 1800:
                return 400 + (x - 1800) * 100 / 600
            else:
                return 0

        final_df["NH3_AQI"] = final_df["NH3"].apply(lambda x: get_NH3_subindex(x)).round(2)

        def get_CO_subindex(x):
            if x <= 1:
                return x * 50 / 1
            elif x <= 2:
                return 50 + (x - 1) * 50 / 1
            elif x <= 10:
                return 100 + (x - 2) * 100 / 8
            elif x <= 17:
                return 200 + (x - 10) * 100 / 7
            elif x <= 34:
                return 300 + (x - 17) * 100 / 17
            elif x > 34:
                return 400 + (x - 34) * 100 / 17
            else:
                return 0

        final_df["CO_AQI"] = final_df["CO"].apply(lambda x: get_CO_subindex(x)).round(2)

        def get_ozone_subindex(x):
            if x <= 50:
                return x * 50 / 50
            elif x <= 100:
                return 50 + (x - 50) * 50 / 50
            elif x <= 168:
                return 100 + (x - 100) * 100 / 68
            elif x <= 208:
                return 200 + (x - 168) * 100 / 40
            elif x <= 748:
                return 300 + (x - 208) * 100 / 539
            elif x > 748:
                return 400 + (x - 400) * 100 / 539
            else:
                return 0

        final_df["OZONE_AQI"] = final_df["OZONE"].apply(lambda x: get_ozone_subindex(x)).round(2)


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

        final_df["Checks"] = (final_df["PM2.5_AQI"] > 0).astype(int) + \
                        (final_df["PM10_AQI"] > 0).astype(int) + \
                        (final_df["SO2_AQI"] > 0).astype(int) + \
                        (final_df["NO2_AQI"] > 0).astype(int) + \
                        (final_df["NH3_AQI"] > 0).astype(int) + \
                        (final_df["CO_AQI"] > 0).astype(int) + \
                        (final_df["OZONE_AQI"] > 0).astype(int)

        final_df["AQI"] = round(final_df[["PM2.5_AQI", "PM10_AQI", "SO2_AQI", "NO2_AQI","NH3_AQI", "CO_AQI", "OZONE_AQI"]].max(axis = 1))
        final_df.loc[final_df["PM2.5_AQI"] + final_df["PM10_AQI"] <= 0, "AQI"] = np.NaN
        final_df.loc[final_df.Checks < 3, "AQI"] = np.NaN

        final_df["AQI_Quality"] = final_df["AQI"].apply(lambda x: get_AQI_bucket(x))

        # columns_to_drop = ["PM2.5_AQI", "PM10_AQI", "SO2_AQI", "NO2_AQI","NH3_AQI", "CO_AQI", "OZONE_AQI","Checks"]
        # final_df.drop(columns_to_drop, axis=1, inplace=True)

        current_datetime = datetime.now().strftime('%Y%m%d')
        output_file_path = output_path + f'/Gold_pollutiondata_{current_datetime}.csv'
        final_df.to_csv(output_file_path, index=False)
