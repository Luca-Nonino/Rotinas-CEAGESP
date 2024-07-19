import os
import json
from pathlib import Path
from email.header import decode_header

def decode_filename(encoded_string):
    """Decode MIME encoded filenames and normalize them."""
    decoded_string, encoding = decode_header(encoded_string)[0]
    if encoding:
        decoded_string = decoded_string.decode(encoding)
    
    # Normalize filename to avoid encoding mismatches
    return decoded_string.replace("Cotação", "Cotacao")

def ensure_file_initialized(processed_list_path):
    """Ensure the JSON file is initialized properly."""
    if not processed_list_path.exists() or processed_list_path.stat().st_size == 0:
        with processed_list_path.open('w') as file:
            json.dump({"files": []}, file, indent=4)

def update_processed_list(processed_list_path, filename):
    """Update the processed list with new file information."""
    ensure_file_initialized(processed_list_path)
    with processed_list_path.open('r+') as file:
        processed_list = json.load(file)
        if not any(entry['name'] == filename for entry in processed_list['files']):
            processed_list['files'].append({"name": filename, "status": "UNPROCESSED"})
            file.seek(0)
            json.dump(processed_list, file, indent=4)
            file.truncate()
        print(f"Updated processed list with {filename}.")

def scan_directory(directory, processed_list_path):
    """Scan the directory for new files and update the processed list."""
    processed_list_path = Path(processed_list_path)
    directory = Path(directory)
    ensure_file_initialized(processed_list_path)
    for file in directory.iterdir():
        if file.is_file():
            normalized_name = decode_filename(file.name)
            if not is_file_processed(processed_list_path, normalized_name):
                update_processed_list(processed_list_path, normalized_name)

def is_file_processed(processed_list_path, filename):
    """Check if the file has already been processed."""
    ensure_file_initialized(processed_list_path)
    with processed_list_path.open('r') as file:
        processed_list = json.load(file)
        return any(entry['name'] == filename for entry in processed_list['files'])

if __name__ == "__main__":
    data_directory = "data/raw"
    processed_files_log = "data/logs/processed_list.json"

    print("Scanning directory for new files...")
    scan_directory(data_directory, processed_files_log)
    print("Scanning complete.")
