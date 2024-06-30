import mysql.connector
from datetime import datetime, timedelta

class DataCleaning:
    @staticmethod
    def DataTransferSQL(year, month, day):
        db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': 'admin',
            'database': 'udyaansaathidata',
            'connection_timeout': 600  # Set a longer timeout
        }

        # Create the date object and subtract one day
        date_obj = datetime(year, month, day) - timedelta(days=1)
        date_str = date_obj.strftime('%Y-%m-%d 00:00:00')
        
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        table_name = "hourlydata"
        clean_query = f"DELETE FROM {table_name} WHERE Pol_Date < '{date_str}'"

        # Execute the query
        cursor.execute(clean_query)
        connection.commit()  # Commit the transaction

        # Close the connection
        cursor.close()
        connection.close()

# Example usage
DataCleaning.DataTransferSQL(2024, 6, 27)
