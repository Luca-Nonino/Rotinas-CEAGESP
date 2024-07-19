import json
import sys

def main():
    try:
        with open('data/logs/processed_list.json', 'r') as f:
            processed_list = json.load(f)
    except FileNotFoundError:
        print("Error: processed_list.json not found.")
        sys.exit(1)

    unprocessed_files = [file for file in processed_list['files'] if file['status'] != 'PROCESSED']

    if unprocessed_files:
        print(f"Error: {len(unprocessed_files)} file(s) not processed successfully.")
        for file in unprocessed_files:
            print(f"- {file['name']}")
        sys.exit(1) # Return a non-zero exit status if there are unprocessed files
    else:
        print("All files processed successfully.")
        sys.exit(0) # Return a zero exit status if all files are processed

if __name__ == '__main__':
    main()
