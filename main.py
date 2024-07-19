import subprocess
import sys

# Paths to the scripts
script_paths = [
    "scripts/email_sync.py",
    "scripts/check_completion.py",
    "scripts/add_entry_reference.py",
    "scripts/file_conversion.py",
    "scripts/check_cod_ipv.py",
]

# Executing each script sequentially
for script_path in script_paths:
    print(f"Executando {script_path}...")
    try:
        output = subprocess.check_output(["python", script_path], text=True)
        print(output)
        if script_path == "scripts/check_completion.py" and "All files processed successfully." in output:
            print("Pipeline concluído com sucesso.")
            break # Stop execution if all files are processed
    except subprocess.CalledProcessError as e:
        print(f"{script_path} failed with exit status {e.returncode}. Continuing with the next script.")

if script_path != "scripts/check_completion.py":
    print("Pipeline concluído com sucesso.")
