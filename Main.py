import subprocess
# this file is use to run all the ETL pipeline
from Silver_DataCleansing import Silver
from Gold_DataTransformation import Gold
from Platinum_FInalData import Platinum
Silver.Datacleansing()
Gold.DataTransformation()
Platinum.FinalData()