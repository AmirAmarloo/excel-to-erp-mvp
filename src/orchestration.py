import sys
import os
import pandas as pd
from engine.loader import load_config, load_data
from engine.validators import process_datetime, check_pattern
from engine.rules import validate_custom_rule
from engine.reporter import save_results

def run_pipeline():
    """
    Final Orchestration: Manages the full lifecycle of data validation.
    """
    # Dynamic paths
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(BASE_DIR, "..", "mappings", "mapping.yaml")
    output_dir = os.path.join(BASE_DIR, "..", "data", "result")

    # Load Configuration and Excel Data
    config = load_config(config_path)
    df = load_data(config)
    
    errors = []
    valid_rows = []

    for index, row in df.iterrows():
        row_num = index + 2
        row_has_error = False
        processed_row = {}
        
        for col_name, rules in config["columns"].items():
            value = row.get(col_name)
            priority = rules.get("priority", 99)
            
            # 1. Type Conversion
            try:
                if rules["type"] == "datetime":
                    current_val = process_datetime(value, rules)
                elif rules["type"] == "float":
                    current_val = float(str(value).strip())
                elif rules["type"] == "int":
                    current_val = int(float(str(value).strip()))
                else:
                    current_val = str(value).strip() if not pd.isna(value) else value

                # 2. Mandatory Check
                if rules.get("required") and pd.isna(current_val):
                    raise ValueError("This field is required but missing.")

                # 3. Custom DSL Rules (Length, Allowed Values, Conditional, Date Range)
                if "custom_rule" in rules and not pd.isna(current_val):
                    rule_config = rules["custom_rule"]
                    if not validate_custom_rule(rule_config, current_val, row):
                        errors.append({
                            "Row_Number": row_num, "Severity": priority, "Column_Name": col_name, 
                            "Invalid_Value": value, "Error_Type": "BUSINESS_RULE_VIOLATION", 
                            "Error_Message": f"Failed {rule_config['type']} validation"
                        })
                        row_has_error = True

                processed_row[col_name] = current_val

            except Exception as e:
                errors.append({
                    "Row_Number": row_num, "Severity": priority, "Column_Name": col_name, 
                    "Invalid_Value": value, "Error_Type": "VALIDATION_ERROR", "Error_Message": str(e)
                })
                row_has_error = True

        if not row_has_error:
            valid_rows.append(processed_row)

    # Export results
    log = save_results(valid_rows, errors, output_dir, config)
    print(log)

if __name__ == "__main__":
    print("🚀 Running Orchestration with all Business Rules...")
    try:
        run_pipeline()
        print("✨ Process finished. Check data/result folder.")
    except Exception as e:
        print(f"❌ System Failure: {e}")
