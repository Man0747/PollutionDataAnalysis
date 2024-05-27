import pandas as pd

csv_file_path = 'F:\\Education\\COLLEGE\\PROGRAMING\\Python\\PROJECTS\\PollutionDataAnalysisProject\\Platinum_Hour\\pollutiondata_Final.csv'

# Read the CSV file
final_df = pd.read_csv(csv_file_path)

# Columns to clean
columns_to_clean = ['Latitude', 'Longitude']

# Clean 'NA' and blank columns and convert to numeric
for column in columns_to_clean:
    # Replace 'NA' strings and blank spaces with NaN
    final_df[column] = final_df[column].replace('NA', pd.NA)
    final_df[column] = final_df[column].replace('', pd.NA)
    # Convert to numeric, coercing errors will turn any non-numeric values into NaN
    final_df[column] = pd.to_numeric(final_df[column], errors='coerce')
    # Fill NaN values with 0
    final_df[column] = final_df[column].fillna(0)

# Save the cleaned DataFrame back to the CSV file
final_df.to_csv(csv_file_path, index=False)
