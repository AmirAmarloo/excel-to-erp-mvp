import pandas as pd
import yaml
import os

def load_config(config_path="mappings/mapping.yaml"):
    """
    Loads the YAML configuration file containing mapping and validation rules.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
        
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def load_source_chunks(config, chunk_size=50000):
    """
    Loads data from CSV or Excel in manageable chunks.
    Optimized for memory efficiency (In-memory buffering).
    """
    file_path = config["source"]["file"]
    sheet_name = config["source"].get("sheet", 0)
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Source file not found at: {file_path}")

    file_ext = os.path.splitext(file_path)[1].lower()

    # Case 1: CSV File (Optimized for 1.5M+ rows)
    if file_ext == '.csv':
        print(f"⚡ True Chunking enabled for CSV: {os.path.basename(file_path)}")
        # Adding encoding='utf-8' for international character support (GmbH, Müller, etc.)
        return pd.read_csv(file_path, chunksize=chunk_size, encoding='utf-8')

    # Case 2: Excel File
    else:
        print(f"📖 Streaming Excel Data: {os.path.basename(file_path)}")
        # Note: Pandas read_excel doesn't support native chunking from disk
        # We load it and yield it in chunks to keep orchestration consistent
        full_df = pd.read_excel(file_path, sheet_name=sheet_name)
        
        def excel_generator():
            for i in range(0, len(full_df), chunk_size):
                yield full_df.iloc[i:i + chunk_size]
        
        return excel_generator()

def get_result_path(config):
    """
    Determines the output directory for results.
    """
    return "data/result"