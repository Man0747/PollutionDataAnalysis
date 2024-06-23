import glob
import pandas as pd
import csv
import requests
import os
from datetime import datetime
import schedule
import time

def getData():
    
    API_URL = 'https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69?api-key=579b464db66ec23bdd000001db6f8d31eff0408478fc83710b8c735f&format=csv&offset=100&limit=2000'

    output_Path = 'F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/PollutionDataAnalysisProject/Bronze'
    year = str(datetime.now().year)
    month = str(datetime.now().month)
    day = str(datetime.now().day)

    output_Path = output_Path + "/" + year + "/" + month + "/" + day

    isExist = os.path.exists(output_Path)

    if not isExist:
        os.makedirs(output_Path)
        
    with requests.Session() as s:
        download = s.get(API_URL)
        decoded_content = download.content

        # Generate the new filename with date and time separated by hyphens
        current_datetime = datetime.now().strftime('%Y%m%d_%H%M')
        new_filename = output_Path + f'/pollutiondata_{current_datetime}.csv'

        csv_file = open(new_filename, 'wb')
        csv_file.write(decoded_content)
        csv_file.close()

# Schedule the getData method to run every hour
schedule.every(1).hour.do(getData)

# Run the scheduler
while True:
    schedule.run_pending()
    time.sleep(1800)

