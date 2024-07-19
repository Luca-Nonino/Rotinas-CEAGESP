import pandas as pd
import os

def load_ipvs_files():
    # Adjust the ipvs_files path to be relative to the script's location
    ipvs_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'processed', 'IPVS')
    ipvs_files = [f for f in os.listdir(ipvs_folder) if f.startswith('IPVS')]
    dfs = []
    for file in ipvs_files:
        df = pd.read_csv(os.path.join(ipvs_folder, file), delimiter=',')
        df.rename(columns={df.columns[0]: 'cod'}, inplace=True)
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def verify_cod_values(id_reference_df, ipvs_df):
    missing_cods = id_reference_df[~id_reference_df['cod'].isin(ipvs_df['cod'])]
    if not missing_cods.empty:
        print("Missing cod values in IPVS files:")
        print(missing_cods[['cod', 'UNIQUE_CONCAT']])
    else:
        print("All cod values from id_reference.csv are present in IPVS files.")

if __name__ == "__main__":
    # Adjust the id_reference_df path to be relative to the script's location
    id_reference_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'processed', 'id_reference.csv')
    id_reference_df = pd.read_csv(id_reference_path, delimiter=';')
    ipvs_df = load_ipvs_files()
    verify_cod_values(id_reference_df, ipvs_df)
