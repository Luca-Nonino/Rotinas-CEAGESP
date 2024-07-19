# -*- coding: utf-8 -*-
import os
import pandas as pd
import openpyxl
from openpyxl import load_workbook
import json
import re
import unicodedata

def convert_to_ipvs(file_paths):
    for file_path in file_paths:
        df = load_file(file_path)
        
        df.rename(columns={'Guerra Produto + Classificação': '<cod>'}, inplace=True)
        for column in ['Produto', 'Variedade', 'Classificação']:
            df[column] = df[column].fillna('N/A').str.replace(' ', '_')
        
        df['<concat>'] = df['Produto'] + df['Variedade'] + df['Classificação']
        
        id_reference_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'processed', 'id_reference.csv')
        df = update_cod_using_reference(df, id_reference_path)
        
        for index, row in df.iterrows():
            if row['Unidade'] in ['KG', 'MC']:
                df.at[index, '<min>'], df.at[index, '<ult>'], df.at[index, '<max>'] = row['Menor'], row['Comum'], row['Maior']
            elif row['Unidade'] in ['ENG', 'UN']:
                df.at[index, '<min>'], df.at[index, '<ult>'], df.at[index, '<max>'] = row['Menor'] / row['Peso'], row['Comum'] / row['Peso'], row['Maior'] / row['Peso']
            elif row['Unidade'] == 'DZMC':
                df.at[index, '<min>'], df.at[index, '<ult>'], df.at[index, '<max>'] = row['Menor'] / 12, row['Comum'] / 12, row['Maior'] / 12
        
        df = df[df['<cod>'] != 'Unknown']
        
        df.rename(columns={'Data': '<data>'}, inplace=True)
        df = df[['<cod>', '<data>', '<min>', '<ult>', '<max>']]
        save_as_ipvs(df, file_path)

def load_file(file_path):
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file {file_path} does not exist.")
        
        workbook = load_workbook(filename=file_path)
        sheet = workbook.active
        
        df = pd.DataFrame(sheet.values)
        df.columns = df.iloc[0]
        df = df.iloc[1:]
        
        return df
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        raise

def update_cod_using_reference(df, reference_path):
    try:
        reference_df = pd.read_csv(reference_path, delimiter=';')
        cod_mapping = dict(zip(reference_df['UNIQUE_CONCAT'], reference_df['cod']))
        df['<cod>'] = df['<concat>'].apply(lambda x: cod_mapping.get(x, 'Unknown'))
    except Exception as e:
        print(f"Error updating cod using reference: {e}")
        raise
    return df

def save_as_ipvs(df, original_file_path):
    try:
        save_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'processed', 'IPVS')
        os.makedirs(save_folder, exist_ok=True)
        
        if '<data>' in df.columns:
            df['<data>'] = pd.to_datetime(df['<data>']).dt.strftime('%Y-%m-%d')
        
        date_pattern = r"\d{2}\.\d{2}\.\d{4}"
        match = re.search(date_pattern, original_file_path)
        if match:
            date_str = match.group(0).replace('.', '')
        else:
            raise ValueError(f"Date not found in file name: {original_file_path}")
        
        save_path = os.path.join(save_folder, f"IPVS_{date_str}.csv")
        print(f"Saving file to: {save_path}")
        df.to_csv(save_path, index=False, header=['<cod>', '<data>', '<min>', '<ult>', '<max>'])
        mark_file_as_processed(os.path.basename(original_file_path))
    except Exception as e:
        print(f"Error saving IPVS file: {e}")
        raise


def mark_file_as_processed(filename):
    processed_list_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'logs', 'processed_list.json')
    try:
        with open(processed_list_path, 'r', encoding='utf-8') as file:
            processed_list = json.load(file)

        normalized_filename = normalize_filename(filename)
        for item in processed_list['files']:
            if normalize_filename(item['name']) == normalized_filename:
                item['status'] = 'PROCESSED'
                break

        with open(processed_list_path, 'w', encoding='utf-8') as file:
            json.dump(processed_list, file, indent=4, ensure_ascii=False)
        print(f"Marked {filename} as PROCESSED")
    except Exception as e:
        print(f"Error updating processed list: {e}")
        raise
def normalize_filename(filename):
    return ''.join(c for c in unicodedata.normalize('NFD', filename)
                   if unicodedata.category(c) != 'Mn').lower()

def get_unprocessed_file_paths():
    processed_list_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'logs', 'processed_list.json')
    raw_data_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'raw')
    try:
        with open(processed_list_path, 'r', encoding='utf-8') as file:
            processed_list = json.load(file)
        
        unprocessed_files = []
        for item in processed_list['files']:
            if item['status'] == 'UNPROCESSED':
                normalized_name = normalize_filename(item['name'])
                # Search for the file in the raw data folder
                for filename in os.listdir(raw_data_folder):
                    if normalize_filename(filename) == normalized_name:
                        full_path = os.path.join(raw_data_folder, filename)
                        unprocessed_files.append(full_path)
                        break
        
        print(f"Unprocessed files: {unprocessed_files}")
    except Exception as e:
        print(f"Error fetching unprocessed file paths: {e}")
        raise
    return unprocessed_files

if __name__ == "__main__":
    unprocessed_files = get_unprocessed_file_paths()
    convert_to_ipvs(unprocessed_files)
