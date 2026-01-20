import pandas as pd
import yaml

def load_config(path):
    """Loads YAML mapping configuration."""
    with open(path, "r") as f:
        return yaml.safe_load(f)

def load_data(config):
    """Loads Excel data based on config paths."""
    df = pd.read_excel(config["source"]["file"], sheet_name=config["source"]["sheet"])
    df.columns = [str(c).strip() for c in df.columns]
    return df
