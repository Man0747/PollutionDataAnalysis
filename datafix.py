import pandas as pd
import os
import glob
from datetime import datetime 
root_directory = "F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/PollutionDataAnalysisProject/Bronze"

# Traverse the directory structure
for year_folder in os.listdir(root_directory):
    year_path = os.path.join(root_directory, year_folder)
    # year_path = 2023

    if os.path.isdir(year_path):
        for month_folder in os.listdir(year_path):
            month_path = os.path.join(year_path, month_folder)
            if os.path.isdir(month_path):
                for day_folder in os.listdir(month_path):
                    day_path = os.path.join(month_path, day_folder)
                    if os.path.isdir(day_path):
                        # Construct the target date based on the directory structure
                        date_parts = [day_folder, month_folder, year_folder]
                       
                        target_date = datetime.strptime("-".join(date_parts), "%d-%m-%Y").strftime("%d-%m-%Y")
                        print(f"Processing date: {target_date}")

                        csv_files = glob.glob(os.path.join(day_path, "*.csv"))

                        for csv_file in csv_files:
                            # Load each CSV file into a DataFrame
                            with open(csv_file, 'r') as file:
                                data = file.read()
                                data = data.replace(",", "\t")  # Replace commas with tabs
                                # You may need additional data cleaning steps here

                            # Create a new file with the preprocessed data
                            new_file_name = os.path.splitext(csv_file)[0] + "_preprocessed.csv"
                            with open(new_file_name, 'w') as new_file:
                                new_file.write(data)

                            try:
                                df = pd.read_csv(new_file_name, delimiter='\t')

                                # Check if 'last_update' column exists in the DataFrame
                                if 'last_update' in df.columns:
                                    # Extract the date part from the 'last_update' column using the new date format
                                    df['date_part'] = df['last_update'].str.split(' ').str[0]

                                    # Check if the target_date is present in the 'date_part' column
                                    if target_date in df['date_part'].values:
                                        print(f"Date {target_date} found in {csv_file}. Keeping the file as is.")
                                        os.remove(new_file_name)
                                    else:
                                        # Rename the file to include "anomaly" and the date from the file
                                        file_name, file_extension = os.path.splitext(csv_file)
                                        new_file_name2 = f"anomaly_{df['date_part'].iloc[0].replace('-', '_')}{file_extension}"

                                        # Rename the file
                                        os.rename(csv_file, os.path.join(day_path, new_file_name2))
                                        print(f"Date {target_date} not found in {csv_file}. Renamed the file to {new_file_name2}")
                                        os.remove(new_file_name2)
                                        os.remove(new_file_name)
                                else:
                                    print(f"'last_update' column not found in {csv_file}. Skipping the file.")
                                    os.remove(new_file_name)
                            except Exception as e:
                                print(f"Error processing {csv_file}: {str(e)}")

# Define the target date
# target_date = "18-10-2020"  # Only the date part without time

# Specify the folder where the CSV files are located
# folder_path = "F:/Education/COLLEGE/PROGRAMING/Python/PROJECTS/PollutionDataAnalysisProject/Bronze/2020/10/18" 

# Get a list of CSV files in the folder
# csv_files = glob.glob(os.path.join(folder_path, "*.csv"))

# for csv_file in csv_files:
#     # Load each CSV file into a DataFrame
#     with open(csv_file, 'r') as file:
#         data = file.read()
#         data = data.replace(",", "\t")  # Replace commas with tabs
#         # You may need additional data cleaning steps here

#     # Create a new file with the preprocessed data
#     new_file_name = os.path.splitext(csv_file)[0] + "_preprocessed.csv"
#     with open(new_file_name, 'w') as new_file:
#         new_file.write(data)

#     try:
#         df = pd.read_csv(new_file_name, delimiter='\t')

#         # Check if 'last_update' column exists in the DataFrame
#         if 'last_update' in df.columns:
#             # Extract the date part from the 'last_update' column using the new date format
#             df['date_part'] = df['last_update'].str.split(' ').str[0]

#             # Check if the target_date is present in the 'date_part' column
#             if target_date in df['date_part'].values:
#                 print(f"Date {target_date} found in {csv_file}. Keeping the file as is.")
#                 os.remove(new_file_name)
#             else:
#                 # Rename the file to include "anomaly" and the date from the file
#                 file_name, file_extension = os.path.splitext(csv_file)
#                 new_file_name2 = f"anomaly_{df['date_part'].iloc[0].replace('-', '_')}{file_extension}"

#                 # Rename the file
#                 os.rename(csv_file, os.path.join(folder_path, new_file_name2))
#                 print(f"Date {target_date} not found in {csv_file}. Renamed the file to {new_file_name2}")
#                 os.remove(new_file_name)
#         else:
#             print(f"'last_update' column not found in {csv_file}. Skipping the file.")
#             os.remove(new_file_name)
#     except Exception as e:
#         print(f"Error processing {csv_file}: {str(e)}")
