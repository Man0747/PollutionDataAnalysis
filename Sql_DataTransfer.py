import mysql.connector
import pandas as pd


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






# MySQL Database Configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Impetus@123',
    'database': 'PollutionData'
}

# CSV file path
csv_file_path = 'F:\Education\COLLEGE\PROGRAMING\Python\PROJECTS\PollutionDataAnalysisProject\Platinum\pollutiondata_Final.csv'

# Connect to MySQL
connection = mysql.connector.connect(**db_config)
cursor = connection.cursor()

# Read the CSV file into a pandas data frame
final_df = pd.read_csv(csv_file_path)

final_df['Pol_Date'] = pd.to_datetime(final_df['Pol_Date'], format='%m/%d/%Y', errors='coerce').dt.strftime('%Y-%m-%d')


table_name = "pollutiondata.udyansaathiapi_pollutoin"

# Insert data from the data frame into MySQL
for _, row in final_df.iterrows():
    insert_query = f'INSERT INTO {table_name} ({", ".join(final_df.columns)}) VALUES ({", ".join(["%s" for _ in final_df.columns])})'
    cursor.execute(insert_query, tuple(row))

# Commit changes and close the connection
connection.commit()
connection.close()





