import pandas as pd
import yaml
import os

# Resolve Project Root based on the current file location
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.normpath(os.path.join(BASE_DIR, "..", ".."))

def load_config(config_path=None):
    """
    Loads the YAML configuration file.
    Default path: mappings/mapping.yaml
    """
    if config_path is None:
        config_path = os.path.join(ROOT_DIR, "mappings", "mapping.yaml")
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
        
    with open(config_path, 'r', encoding='utf-8') as file:
        return yaml.load(file, Loader=yaml.SafeLoader)

def load_source_chunks(config, chunk_size=50000):
    """
    Smart Data Loader:
    - CSV: Uses Native Chunking (Directly from disk, low RAM usage).
    - Excel: Loads the sheet then yields chunks (Memory streaming).
    """
    raw_path = config["source"]["file"]
    file_path = raw_path if os.path.isabs(raw_path) else os.path.join(ROOT_DIR, raw_path)
    file_path = os.path.normpath(file_path)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Source file not found at: {file_path}")

    file_ext = os.path.splitext(file_path)[1].lower()
    sheet_name = config["source"].get("sheet", 0)

    # Case 1: CSV File (Optimized for 1.5M+ rows)
    if file_ext == '.csv':
        print(f"⚡ True Chunking enabled for CSV: {os.path.basename(file_path)}")
        return pd.read_csv(file_path, chunksize=chunk_size)

    # Case 2: Excel File
    else:
        print(f"📖 Streaming Excel Data: {os.path.basename(file_path)}")
        # Note: Pandas read_excel doesn't support native chunking from disk
        full_df = pd.read_excel(file_path, sheet_name=sheet_name)
        
        def excel_generator():
            for i in range(0, len(full_df), chunk_size):
                yield full_df.iloc[i:i + chunk_size]
        
        return excel_generator()

def get_result_path(config):
    """Returns the directory path where output files will be saved."""
    return os.path.join(ROOT_DIR, "data", "result")