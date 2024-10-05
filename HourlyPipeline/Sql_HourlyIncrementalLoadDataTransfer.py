import pandas as pd
import mysql.connector
import glob
import os
from datetime import datetime

class HourlyImplementLoadDataTransfer:

    @staticmethod
    def DataTransferSQL(year, month, day, keyword):
        local_db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': 'admin',
            'database': 'udyaansaathidata',
            'connection_timeout': 600  # Set a longer timeout
        }

        azure_db_config = {
            'host': 'mysqlmannan01.mysql.database.azure.com',
            'user': 'mannan',
            'password': 'Khetan@123',
            'database': 'udyaansaathidata',
            'client_flags': [mysql.connector.ClientFlag.SSL],
            'ssl_ca': 'F:\Education\COLLEGE\PROGRAMING\Python\Codes\PolutionDataAnalysis\Devlopment\PollutionDataAnalysis\DigiCertGlobalRootG2.crt.pem'
        }

        db_config = azure_db_config if keyword.lower() == 'azure' else local_db_config

        path = 'F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/PollutionDataAnalysisProject'

        input_path = f"{path}/Gold_Hour/{year}/{month}/{day}"
        input_files = glob.glob(input_path + "/*.csv")
        
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        
        # Get already processed files from the database
        cursor.execute("SELECT file_name FROM processed_files")
        processed_files = set(row[0] for row in cursor.fetchall())

        for csv_file in input_files:
            if csv_file in processed_files:
                print(f"File {csv_file} already processed. Skipping...")
                continue
            
            print("Processing File", csv_file)


            final_df = pd.read_csv(csv_file)
            print(final_df['Date'])

            # Rename the columns
            final_df.rename(columns={"Date": "Pol_Date", "PM2.5": "PM25"}, inplace=True)

            # Correctly parse the date with the correct format
            final_df['Pol_Date'] = pd.to_datetime(final_df['Pol_Date'], format='%d-%m-%Y %H:%M:%S', errors='coerce')

            # Format the date as 'YYYY-MM-DD HH:MM:SS'
            final_df['Pol_Date'] = final_df['Pol_Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
            print(final_df['Pol_Date'])



            table_name = "hourlydata"
            batch_size = 1000  # Adjust batch size as needed
            insert_query = f'INSERT INTO {table_name} ({", ".join(final_df.columns)}) VALUES ({", ".join(["%s" for _ in final_df.columns])})'

            for start in range(0, len(final_df), batch_size):
                end = start + batch_size
                batch_data = final_df.iloc[start:end].values.tolist()
                cursor.executemany(insert_query, batch_data)
                connection.commit()

            # Insert processed file record into the database
            processed_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute("INSERT INTO processed_files (file_name, processed_time) VALUES (%s, %s)", (csv_file, processed_time))
            connection.commit()

        # Optionally, update last update table
        # last_update_table = "hourlypollutiondatalastupdate"
        # last_update_query = f"INSERT INTO {last_update_table} (updated_date) VALUES ('{year}-{month:02d}-{day:02d}')"
        # cursor.execute(last_update_query)
        # connection.commit()

        connection.close()

# Example usage:
# HourlyImplementLoadDataTransfer.DataTransferSQL(2024, 7, 1, 'azure')



