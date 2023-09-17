import glob
import pandas as pd
import csv
import requests
import os
from datetime import datetime

class Platinum:
    def FinalData() :
        path ='F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/PollutionDataAnalysisProject'

        input_path = path + '/Gold'
        year = str(datetime.now().year)
        month = str(datetime.now().month)
        day = str(datetime.now().day - 1)
        # input_path = input_path + "/" + year + "/" + month + "/" + day
        input_path = input_path + "/" + year + "/" + month 
        output_path = path + '/Platinum'
        isExist = os.path.exists(output_path)
        if not isExist:
            os.makedirs(output_path)


        csv_files = glob.glob(input_path + "*/*.csv")

        df_list = (pd.read_csv(file) for file in csv_files)

        combined_df = pd.concat(df_list, ignore_index=True)
        final_df = combined_df


        output_file_path = output_path + f'/pollutiondata_Final.csv'
        final_df.to_csv(output_file_path,mode='a', index=False)

Platinum.FinalData()