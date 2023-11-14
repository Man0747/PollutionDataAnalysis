import subprocess
import pandas as pd
import mysql.connector

# print(mydb)
# mycursor = mydb.cursor()

# mycursor.execute("CREATE TABLE PollutionData (State_ID INT(50),City_ID	SMALLINT,Station_ID	SMALLINT,\
#                   Date	DATE, CO	FLOAT(10), NH3	FLOAT(10),NO2	FLOAT(10), OZONE	FLOAT(10),\
#                   PM10	FLOAT(10),PM25 FLOAT(10), SO2 FLOAT(10), Checks INT(10), AQI FLOAT(10), \
#                   AQI_Quality VARCHAR(100),\
#                   PRIMARY KEY(Station_ID, Date))  ")
													
# mycursor.execute("CREATE TABLE StateData (State VARCHAR(255),State_ID INT(50),PRIMARY KEY(State_ID))")

# mycursor.execute("CREATE TABLE CityData (City VARCHAR(255),City_ID SMALLINT ,PRIMARY KEY(City_ID))")

# mycursor.execute("CREATE TABLE StationData (Station VARCHAR(500),Station_ID SMALLINT ,PRIMARY KEY(Station_ID))")



# Apply the custom parser to the date column


class DataTransfer:
    def convert_date(date):
        try:
            # Try parsing the date with "/" separator
            return pd.to_datetime(date, format='%m/%d/%Y').strftime('%Y-%m-%d')
        except ValueError:
            # If parsing fails, return the original date
            return date
        
    def DataTransferSQL():

# MySQL Database Configuration
        db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': 'Impetus@123',
            'database': 'pollutiondata'
        }

        # CSV file path
        csv_file_path = 'F:\Education\COLLEGE\PROGRAMING\Python\PROJECTS\PollutionDataAnalysisProject\Platinum\pollutiondata_Final.csv'

        # Connect to MySQL
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # df = pd.read_csv(csv_file_path, converters={'Pol_Date': custom_date_parser})
        # Read the CSV file into a pandas data frame
        final_df = pd.read_csv(csv_file_path)
        # df = pd.read_csv(csv_file_path, parse_dates=['Date'], date_parser=lambda x: pd.to_datetime(x, format='%Y-%m-%d'))
        final_df = pd.read_csv(csv_file_path, parse_dates=['Date'], infer_datetime_format=True)
        final_df.rename(columns={"Date": "Pol_Date","PM2.5": "PM25"}, inplace=True)
        # final_df['Pol_Date'] = pd.to_datetime(final_df['Pol_Date'], format='%m/%d/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
        # final_df['Pol_Date'] = pd.to_datetime(final_df['Pol_Date'], errors='coerce').dt.strftime('%Y-%m-%d')
        final_df['Pol_Date'] = final_df['Pol_Date'].apply(DataTransfer.convert_date)
        
        # print(final_df["Pol_Date"])
        
        # row_index = 47572
        # single_row = final_df.loc[row_index]

        # print(single_row)

        table_name = "pollutiondata.udyansaathiapi_pollutoin"

        data_delete = f"DELETE FROM {table_name};"
        cursor.execute(data_delete)
        connection.commit()

        # for column in final_df.columns:
            # final_df[column] = pd.to_numeric(final_df[column], errors='coerce')

        # final_df = final_df.where(pd.notna(final_df), None)

        # Insert data from the data frame into MySQL
        for _, row in final_df.iterrows():
            
            insert_query = f'INSERT INTO {table_name} ({", ".join(final_df.columns)}) VALUES ({", ".join(["%s" for _ in final_df.columns])})'
            cursor.execute(insert_query, tuple(row))

        # Commit changes and close the connection
        connection.commit()
        connection.close()
# DataTransfer.DataTransferSQL()




