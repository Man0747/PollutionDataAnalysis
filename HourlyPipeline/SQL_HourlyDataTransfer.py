# import glob
# import numpy as np
# import pandas as pd
# import csv
# import requests
# import os
# from datetime import datetime
# import mysql.connector

# class DataTransfer:
#     # def DataTransferSQL(final_df):
#     # # MySQL Database Configuration
#     #     db_config = {
#     #         'host': 'localhost',
#     #         'user': 'root',
#     #         'password': 'Impetus@123',
#     #         'database': 'pollutiondata'
#     #     }

#     #     # Connect to MySQL
#     #     connection = mysql.connector.connect(**db_config)
#     #     cursor = connection.cursor()

#     #     # Read the CSV file into a pandas data frame
#     #     final_df.rename(columns={"Date": "Pol_Date", "PM2.5": "PM25"}, inplace=True)
#     #     final_df['Pol_Date'] = pd.to_datetime(final_df['Pol_Date'], format='%d-%m-%Y %H:%M:%S', errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

#     #     table_name = "pollutiondata.onlylatesthourdata"

#     #     data_delete = f"DELETE FROM {table_name};"
#     #     cursor.execute(data_delete)
#     #     connection.commit()
#     #     # Explicitly specify column names in the query
#     #     columns_str = ", ".join(final_df.columns)
#     #     values_str = ", ".join(["%s" for _ in final_df.columns])
#     #     insert_query = f'INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})'

#     #     # Insert data from the data frame into MySQL
#     #     for _, row in final_df.iterrows():
#     #         print("Insert Query:", insert_query)
#     #         print("Row Values:", tuple(row))
#     #         cursor.execute(insert_query, tuple(row))

#     #     # Commit changes and c lose the connection
#     #     connection.commit()
#     #     connection.close()

#     # # def DataHouronlyTransferSQL(final_df):
#     # # # MySQL Database Configuration
#     # #     db_config = {
#     # #         'host': 'localhost',
#     # #         'user': 'root',
#     # #         'password': 'Impetus@123',
#     # #         'database': 'pollutiondata'
#     # #     }

#     # #     # Connect to MySQL
#     # #     connection = mysql.connector.connect(**db_config)
#     # #     cursor = connection.cursor()

#     # #     # Read the CSV file into a pandas data frame
#     # #     final_df.rename(columns={"Date": "Pol_Date", "PM2.5": "PM25"}, inplace=True)
#     # #     final_df['Pol_Date'] = pd.to_datetime(final_df['Pol_Date'], format='%d-%m-%Y %H:%M:%S', errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

#     # #     table_name = "pollutiondata.hourlydata"

#     # #     # Explicitly specify column names in the query
#     # #     columns_str = ", ".join(final_df.columns)
#     # #     values_str = ", ".join(["%s" for _ in final_df.columns])
#     # #     insert_query = f'INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})'

#     # #     # Insert data from the data frame into MySQL
#     # #     for _, row in final_df.iterrows():
#     # #         print("Insert Query:", insert_query)
#     # #         print("Row Values:", tuple(row))
#     # #         cursor.execute(insert_query, tuple(row))

#     # #     # Commit changes and close the connection
#     # #     connection.commit()
#     # #     connection.close()
#     # # def convert_date(date):
#     # #     try:
#     # #         return pd.to_datetime(date, format='%m/%d/%Y').strftime('%Y-%m-%d')
#     # #     except ValueError:
#     # #         return date
        
#     def DataTransferSQL():

#         db_config = {
#             'host': 'localhost',
#             'user': 'root',
#             'password': 'admin',
#             'database': 'udyaansaathidata'
#         }

#         csv_file_path = 'F:\Education\COLLEGE\PROGRAMING\Python\PROJECTS\PollutionDataAnalysisProject\Platinum_Hour\pollutiondata_Final.csv'

#         connection = mysql.connector.connect(**db_config)
#         cursor = connection.cursor()

#         final_df = pd.read_csv(csv_file_path)
#         print(final_df["Date"])
#         final_df = pd.read_csv(csv_file_path, parse_dates=['Date'], infer_datetime_format=True)
#         final_df.rename(columns={"Date": "Pol_Date","PM2.5": "PM25"}, inplace=True)
#         final_df['Pol_Date'] = pd.to_datetime(final_df['Pol_Date'], format='%d-%m-%Y %H:%M:%S', errors='coerce ').dt.strftime('%Y-%m-%d %H:%M:%S')


#         table_name = "udyaansaathidata.hourlydata"


#         data_delete = f"DELETE FROM {table_name};"
#         cursor.execute(data_delete)
#         connection.commit()
#         print(final_df["Pol_Date"])
#         for _, row in final_df.iterrows():
            
#             insert_query = f'INSERT INTO {table_name} ({", ".join(final_df.columns)}) VALUES ({", ".join(["%s" for _ in final_df.columns])})'
#             cursor.execute(insert_query, tuple(row))

#         connection.commit()
#         connection.close()
    
        
#     DataTransferSQL()


