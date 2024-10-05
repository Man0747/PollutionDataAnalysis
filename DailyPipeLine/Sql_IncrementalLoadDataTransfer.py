import pandas as pd
import mysql.connector
import glob
class ImplementLoadDataTransfer:
    @staticmethod
    def convert_date(date):
        try:
            return pd.to_datetime(date, format='%m/%d/%Y').strftime('%Y-%m-%d')
        except ValueError:
            return date

    @staticmethod
    def DataTransferSQL(year,month,day,config):
        # local_db_config = {
        #     'host': 'localhost',
        #     'user': 'root',
        #     'password': 'admin',
        #     'database': 'udyaansaathidata',
        #     'connection_timeout': 600  # Set a longer timeout
        # }
        # azure_db_config = {
        #     'host': 'mysqlmannan01.mysql.database.azure.com',
        #     'user': 'mannan',
        #     'password': 'Khetan@123',
        #     'database': 'udyaansaathidata',
        #     'client_flags': [mysql.connector.ClientFlag.SSL],
        #     'ssl_ca': r'F:\Education\COLLEGE\PROGRAMING\Python\Codes\PolutionDataAnalysis\Devlopment\PollutionDataAnalysis\DigiCertGlobalRootG2.crt.pem'
        # }

        # db_config = azure_db_config if keyword.lower() == 'azure' else local_db_config
        db_config = config
        path = 'F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/PollutionDataAnalysisProject'

        input_path = f"{path}/Gold/{year}/{month}/{day}"
        input_path = glob.glob(input_path + "/*.csv")
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        final_df = pd.read_csv(input_path[0], parse_dates=['Date'], infer_datetime_format=True)
        final_df.rename(columns={"Date": "Pol_Date", "PM2.5": "PM25"}, inplace=True)
        final_df['Pol_Date'] = final_df['Pol_Date'].apply(ImplementLoadDataTransfer.convert_date)


        table_name = "pollutiondata"
        batch_size = 1000  # Adjust batch size as needed
        insert_query = f'INSERT INTO {table_name} ({", ".join(final_df.columns)}) VALUES ({", ".join(["%s" for _ in final_df.columns])})'

        for start in range(0, len(final_df), batch_size):
            end = start + batch_size
            batch_data = final_df.iloc[start:end].values.tolist()
            cursor.executemany(insert_query, batch_data)
            connection.commit()

        # last_update_table = "pollutiondatalastupdate"
        # last_update_query = f"INSERT INTO {last_update_table} (updated_date) VALUES ('{year}-{month:02d}-{day:02d}')"
        # cursor.execute(last_update_query)
        # connection.commit()

        connection.close()

# ImplementLoadDataTransfer.DataTransferSQL(2024,6,23,azure)

