import sys
import os
import pandas as pd
from engine.loader import load_config, load_data
from engine.validators import process_datetime, check_pattern
from engine.rules import validate_custom_rule
from engine.reporter import save_results

def run_pipeline():
    """
    Main orchestration function to execute the validation pipeline.
    Includes robust type checking for int and float.
    """
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(BASE_DIR, "..", "mappings", "mapping.yaml")
    output_dir = os.path.join(BASE_DIR, "..", "data", "result")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at: {config_path}")
        
    config = load_config(config_path)
    df = load_data(config)
    
    errors = []
    valid_rows = []
    seen_entries = {}

    for index, row in df.iterrows():
        row_num = index + 2
        row_has_error = False
        processed_row = {}
        
        for col_name, rules in config["columns"].items():
            value = row.get(col_name)
            priority = rules.get("priority", 99)
            
            if rules.get("auto_clean") and isinstance(value, str):
                value = value.strip()

            if rules.get("required") and pd.isna(value):
                errors.append({
                    "Row_Number": row_num, "Severity": priority, "Column_Name": col_name, 
                    "Invalid_Value": "NULL", "Error_Type": "REQUIRED", "Error_Message": "Missing data"
                })
                row_has_error = True
                continue

            if not pd.isna(value):
                try:
                    # Specialized Type Validation
                    if rules["type"] == "datetime":
                        current_val = process_datetime(value, rules)
                    elif rules["type"] == "int":
                        current_val = int(float(str(value)))
                    elif rules["type"] == "float":
                        current_val = float(str(value))
                    else:
                        current_val = value

                    # Logic Validation
                    if rules["type"] == "string" and "pattern" in rules:
                        if not check_pattern(current_val, rules["pattern"]):
                            errors.append({
                                "Row_Number": row_num, "Severity": priority, "Column_Name": col_name, 
                                "Invalid_Value": value, "Error_Type": "PATTERN_MISMATCH", "Error_Message": "Regex failure"
                            })
                            row_has_error = True

                    if "custom_rule" in rules:
                        if not validate_custom_rule(rules["custom_rule"], current_val, row):
                            errors.append({
                                "Row_Number": row_num, "Severity": priority, "Column_Name": col_name, 
                                "Invalid_Value": value, "Error_Type": "CUSTOM_LOGIC_VIOLATION", "Error_Message": "DSL Rule failure"
                            })
                            row_has_error = True

                    processed_row[col_name] = current_val

                except Exception:
                    errors.append({
                        "Row_Number": row_num, "Severity": priority, "Column_Name": col_name, 
                        "Invalid_Value": value, "Error_Type": "INVALID_TYPE", "Error_Message": f"Value is not a valid {rules['type']}"
                    })
                    row_has_error = True

        if not row_has_error:
            valid_rows.append(processed_row)

    log = save_results(valid_rows, errors, output_dir, config)
    print(log)

if __name__ == "__main__":
    print("🚀 Starting Orchestration with Numeric Validation...")
    try:
        run_pipeline()
        print("✅ Done.")
    except Exception as e:
        print(f"❌ Critical Failure: {e}")
