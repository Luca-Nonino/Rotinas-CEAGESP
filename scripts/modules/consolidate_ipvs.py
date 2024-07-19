import pandas as pd
import glob
import os

# Path to the directory containing the CSV files
csv_dir = 'data/processed/IPVS/'

# Use glob to find all CSV files in the directory
files = glob.glob(csv_dir + '*.csv')

# Initialize an empty list to hold DataFrames
dfs = []

# Loop through each file, read it into a DataFrame, and append it to the list
for file in files:
    df = pd.read_csv(file)
    dfs.append(df)

# Concatenate all DataFrames in the list into one DataFrame
combined_df = pd.concat(dfs, ignore_index=True)

# Format <<min>>, <<ult>>, <<max>> columns to 3 decimal places
combined_df['<min>'] = combined_df['<min>'].round(3)
combined_df['<ult>'] = combined_df['<ult>'].round(3)
combined_df['<max>'] = combined_df['<max>'].round(3)

# Save the combined DataFrame to a new CSV file
output_filepath = 'data/processed/IPVS/combined_IPVS.csv'
combined_df.to_csv(output_filepath, index=False)

# Rename the file to have an .ipv extension
os.rename(output_filepath, output_filepath.replace('.csv', '.ipv'))