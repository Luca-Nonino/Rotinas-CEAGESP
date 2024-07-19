import pandas as pd
import os

def load_cotacao_files():
    # Dynamically construct the path to the 'data/raw' directory
    cotacao_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'raw')
    cotacao_files = [f for f in os.listdir(cotacao_folder) if f.startswith('Cotação')]
    dfs = []
    for file in cotacao_files:
        file_path = os.path.join(cotacao_folder, file)
        df = pd.read_excel(file_path, engine='openpyxl')
        # Ensure the column is renamed to '<cod>' before proceeding
        df.rename(columns={'Guerra Produto + Classificação': '<cod>'}, inplace=True)
        # Replace '@' with '_' in the '<cod>' column
        df['<cod>'] = df['<cod>'].str.replace('@', '_')
        # Data Preparation for 'UNIQUE_CONCAT'
        for column in ['Produto', 'Variedade', 'Classificação']:
            df[column] = df[column].fillna('N/A')
            df[column] = df[column].str.replace(' ', '_')
        df['UNIQUE_CONCAT'] = df['Produto'] + df['Variedade'] + df['Classificação']
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def update_id_reference(df):
    # Dynamically construct the path to the 'data/processed/id_reference.csv' file
    id_reference_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'processed', 'id_reference.csv')
    id_reference_df = pd.read_csv(id_reference_path, delimiter=';')
    
    # Ensure the '<cod>' column exists before filtering
    if '<cod>' not in df.columns:
        print("Error: '<cod>' column does not exist in the DataFrame.")
        return
    
    # Filter rows in cotacao DataFrame that have a matching cod in id_reference.csv
    filtered_df = df[df['<cod>'].isin(id_reference_df['cod'])]
    
    # Find new entries in cotacao DataFrame that are not in id_reference.csv
    new_entries = filtered_df[~filtered_df['UNIQUE_CONCAT'].isin(id_reference_df['UNIQUE_CONCAT'])]
    
    if not new_entries.empty:
        # Ensure we only add new entries with existing cod values
        new_entries = new_entries[new_entries['<cod>'].notna()]
        
        # Add new entries to id_reference.csv
        # Ensure the new entries DataFrame has the same columns as the existing one
        new_entries = new_entries[['<cod>', 'UNIQUE_CONCAT']].rename(columns={'<cod>': 'cod'})
        id_reference_df = pd.concat([id_reference_df, new_entries], ignore_index=True)
        id_reference_df.to_csv(id_reference_path, index=False, sep=';')
        print("New entries added to id_reference.csv")
    else:
        print("No new entries found.")

if __name__ == "__main__":
    cotacao_df = load_cotacao_files()
    update_id_reference(cotacao_df)
