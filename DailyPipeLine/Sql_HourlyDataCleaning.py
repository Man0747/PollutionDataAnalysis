import mysql.connector
from datetime import datetime, timedelta

class DataCleaning:
    @staticmethod
    def DataTransferSQL(year, month, day,config):
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
        # Create the date object and subtract one day
        date_obj = datetime(int(year), int(month), int(day)) - timedelta(days=1)
        date_str = date_obj.strftime('%Y-%m-%d 00:00:00')
        
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        table_name = "hourlydata"
        clean_query = f"DELETE FROM {table_name} WHERE Pol_Date < '{date_str}'"

        # Execute the query
        cursor.execute(clean_query)
        connection.commit()  # Commit the transaction

        table_name = "processed_files"
        clean_query = f"DELETE FROM {table_name} WHERE processed_time < '{date_str}'"

        # Execute the query
        cursor.execute(clean_query)
        connection.commit()  # Commit the transaction

        # Close the connection
        cursor.close()
        connection.close()

# Example usage
# DataCleaning.DataTransferSQL(2024, 6, 29,'azure')

