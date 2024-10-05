import pandas as pd
import mysql.connector

class DataTransfer:
    @staticmethod
    def convert_date(date):
        try:
            return pd.to_datetime(date, format='%m/%d/%Y').strftime('%Y-%m-%d')
        except ValueError:
            return date

    @staticmethod
    def DataTransferSQL():
        db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': 'admin',
            'database': 'udyaansaathidata',
            'connection_timeout': 600  # Set a longer timeout
        }

        csv_file_path = 'F:\Education\COLLEGE\PROGRAMING\Python\PROJECTS\PollutionDataAnalysisProject\Platinum\pollutiondata_Final.csv'

        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        final_df = pd.read_csv(csv_file_path, parse_dates=['Date'], infer_datetime_format=True)
        final_df.rename(columns={"Date": "Pol_Date", "PM2.5": "PM25"}, inplace=True)
        final_df['Pol_Date'] = final_df['Pol_Date'].apply(DataTransfer.convert_date)

        table_name = "pollutiondata"

        data_delete = f"DELETE FROM {table_name};"
        cursor.execute(data_delete)
        connection.commit()

        batch_size = 1000  # Adjust batch size as needed
        insert_query = f'INSERT INTO {table_name} ({", ".join(final_df.columns)}) VALUES ({", ".join(["%s" for _ in final_df.columns])})'

        for start in range(0, len(final_df), batch_size):
            end = start + batch_size
            batch_data = final_df.iloc[start:end].values.tolist()
            cursor.executemany(insert_query, batch_data)
            connection.commit()

        connection.close()

# DataTransfer.DataTransferSQL()

