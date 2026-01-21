import sys
import os
import pandas as pd
# Importing internal modules
import warnings
warnings.filterwarnings("ignore", category=UserWarning)
from engine.loader import load_config, load_data
from engine.validators import process_datetime, check_pattern
from engine.rules import validate_custom_rule
from engine.reporter import save_results

def run_pipeline():
    """
    Final Orchestration Source: 
    Coordinates between YAML mapping, data loading, and multi-layered validation.
    """
    
    # 1. SMART PATH RESOLUTION
    # BASE_DIR = .../excel-to-erp-mvp/src
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    # ROOT_DIR = .../excel-to-erp-mvp
    ROOT_DIR = os.path.dirname(BASE_DIR)

    config_path = os.path.join(ROOT_DIR, "mappings", "mapping.yaml")
    output_dir = os.path.join(ROOT_DIR, "data", "result")

    # 2. INITIALIZATION
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")

    config = load_config(config_path)
    
    # Resolve Excel Path dynamically to avoid directory issues
    excel_filename = os.path.basename(config["source"]["file"])
    excel_path = os.path.join(ROOT_DIR, "data", excel_filename)
    
    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"Input Excel not found at: {excel_path}")

    # Load data using the resolved path
    df = pd.read_excel(excel_path, sheet_name=config["source"]["sheet"])
    
    errors = []
    valid_rows = []

    # 3. ROW-BY-ROW VALIDATION
    for index, row in df.iterrows():
        row_num = index + 2 # Excel starts at row 1, headers are row 1
        row_has_error = False
        processed_row = {}
        
        for col_name, rules in config["columns"].items():
            value = row.get(col_name)
            priority = rules.get("priority", 99)
            
            try:
                # A. Type Conversion & Basic Cleaning
                if rules["type"] == "datetime":
                    current_val = process_datetime(value, rules)
                elif rules["type"] == "float":
                    current_val = float(str(value).replace(',', '').strip())
                elif rules["type"] == "int":
                    current_val = int(float(str(value).strip()))
                else:
                    current_val = str(value).strip() if not pd.isna(value) else value

                # B. Mandatory Field Check
                if rules.get("required") and (pd.isna(current_val) or str(current_val) == ""):
                    raise ValueError(f"Column '{col_name}' is mandatory.")

                # C. Custom DSL Rules (Length, Status, Conditional Amount, Date Range)
                if "custom_rule" in rules and not pd.isna(current_val):
                    rule_config = rules["custom_rule"]
                    # We pass 'row' to handle conditional rules (like amount depends on status)
                    if not validate_custom_rule(rule_config, current_val, row):
                        error_msg = f"Failed {rule_config['type']} rule."
                        # Specialized message for better UX
                        if rule_config['type'] == 'conditional':
                            error_msg = f"Value {current_val} invalid because {rule_config['if_col']} is {row.get(rule_config['if_col'])}"
                        
                        errors.append({
                            "Row_Number": row_num, "Severity": priority, "Column_Name": col_name, 
                            "Invalid_Value": value, "Error_Type": "RULE_VIOLATION", 
                            "Error_Message": error_msg
                        })
                        row_has_error = True

                # D. Regex Pattern Check
                if "pattern" in rules and not pd.isna(current_val):
                    if not check_pattern(str(current_val), rules["pattern"]):
                        raise ValueError(f"Does not match required pattern: {rules['pattern']}")

                processed_row[col_name] = current_val

            except Exception as e:
                errors.append({
                    "Row_Number": row_num, "Severity": priority, "Column_Name": col_name, 
                    "Invalid_Value": value, "Error_Type": "VALIDATION_ERROR", "Error_Message": str(e)
                })
                row_has_error = True

        # 4. COLLECTION
        if not row_has_error:
            valid_rows.append(processed_row)

    # 5. REPORTING
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    report_log = save_results(valid_rows, errors, output_dir, config)
    print(report_log)

if __name__ == "__main__":
    print("🚀 Starting Data Pipeline...")
    try:
        run_pipeline()
        print("✅ Process Completed Successfully.")
    except Exception as e:
        print(f"❌ CRITICAL SYSTEM ERROR: {e}")
