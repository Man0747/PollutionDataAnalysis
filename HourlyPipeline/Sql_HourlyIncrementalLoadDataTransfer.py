import pandas as pd
import mysql.connector
import glob
class HourlyImplementLoadDataTransfer:
    # @staticmethod
    # def convert_date(date):
    #     try:
    #         return pd.to_datetime(date, format='%m/%d/%Y').strftime('%Y-%m-%d')
    #     except ValueError:
    #         return date

    @staticmethod
    def DataTransferSQL(year,month,day):
        db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': 'admin',
            'database': 'udyaansaathidata',
            'connection_timeout': 600  # Set a longer timeout
        }

        path = 'F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/PollutionDataAnalysisProject'

        input_path = f"{path}/Gold_Hour/{year}/{month}/{day}"
        input_path = glob.glob(input_path + "/*.csv")
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        for csv_file in input_path:
            final_df = pd.read_csv(csv_file, parse_dates=['Date'], infer_datetime_format=True)
            final_df.rename(columns={"Date": "Pol_Date","PM2.5": "PM25"}, inplace=True)
            final_df['Pol_Date'] = pd.to_datetime(final_df['Pol_Date'], format='%d-%m-%Y %H:%M:%S', errors='coerce ').dt.strftime('%Y-%m-%d %H:%M:%S')


        table_name = "hourlydata"
        batch_size = 1000  # Adjust batch size as needed
        insert_query = f'INSERT INTO {table_name} ({", ".join(final_df.columns)}) VALUES ({", ".join(["%s" for _ in final_df.columns])})'

        for start in range(0, len(final_df), batch_size):
            end = start + batch_size
            batch_data = final_df.iloc[start:end].values.tolist()
            cursor.executemany(insert_query, batch_data)
            connection.commit()

        # last_update_table = "hourlypollutiondatalastupdate"
        # last_update_query = f"INSERT INTO {last_update_table} (updated_date) VALUES ('{year}-{month:02d}-{day:02d}')"
        # cursor.execute(last_update_query)
        # connection.commit()

        connection.close()

# HourlyImplementLoadDataTransfer.DataTransferSQL(2024,6,23)
