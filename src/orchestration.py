import os
import pandas as pd
import warnings
from datetime import datetime
from engine.loader import load_config
from engine.validators import process_datetime, check_pattern
from engine.rules import validate_custom_rule
from engine.reporter import save_results

# Suppress annoying spreadsheet warnings
warnings.filterwarnings("ignore", category=UserWarning)

def run_pipeline():
    """
    Final Orchestrator: Validates Customer Code, Uniqueness, 
    Business Rules, and Data Types without losing previous features.
    """
    # 1. SETUP PATHS
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    ROOT_DIR = os.path.dirname(BASE_DIR)
    
    config_path = os.path.join(ROOT_DIR, "mappings", "mapping.yaml")
    config = load_config(config_path)
    
    excel_path = os.path.join(ROOT_DIR, "data", os.path.basename(config["source"]["file"]))
    
    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"Source file not found at: {excel_path}")

    # 2. LOAD DATA
    df = pd.read_excel(excel_path, sheet_name=config["source"]["sheet"])
    
    errors = []
    valid_rows = []
    seen_values = {} # Dictionary for uniqueness: {'customer_code': set(), 'name': set()}
    start_time = datetime.now()

    # 3. VALIDATION LOOP
    for index, row in df.iterrows():
        row_num = index + 2
        row_has_error = False
        processed_row = {}
        
        for col_name, rules in config["columns"].items():
            val = row.get(col_name)
            col_type = rules.get("type")
            
            try:
                # A. TYPE VALIDATION
                if pd.isna(val) or str(val).strip() == "":
                    current_val = None
                elif col_type == "datetime":
                    current_val = process_datetime(val, rules)
                elif col_type == "float":
                    try:
                        current_val = float(str(val).replace(',', '').strip())
                    except:
                        raise TypeError(f"Invalid numeric type for value: {val}")
                else:
                    current_val = str(val).strip()

                # B. MANDATORY CHECK
                if rules.get("required") and current_val is None:
                    raise ValueError(f"Field '{col_name}' is required.")

                # C. UNIQUENESS CHECK (Ensures Customer Code & Name are unique)
                if rules.get("unique") and current_val is not None:
                    if col_name not in seen_values: seen_values[col_name] = set()
                    if current_val in seen_values[col_name]:
                        raise ValueError(f"Duplicate found in {col_name}: {current_val}")
                    seen_values[col_name].add(current_val)

                # D. PATTERN CHECK (Regex)
                if "pattern" in rules and current_val is not None:
                    if not check_pattern(str(current_val), rules["pattern"]):
                        raise ValueError(f"Pattern mismatch for {col_name}: {current_val}")

                # E. CUSTOM BUSINESS RULES (Length, Suspend, Date Range)
                if "custom_rule" in rules and current_val is not None:
                    if not validate_custom_rule(rules["custom_rule"], current_val, row):
                        raise ValueError(f"Business rule violation: {rules['custom_rule']['type']}")

                processed_row[col_name] = current_val

            except (TypeError, ValueError) as e:
                errors.append({
                    "Row_Number": row_num, "Severity": rules.get("priority", 3),
                    "Column_Name": col_name, "Invalid_Value": val,
                    "Error_Type": "VALIDATION_FAILED", "Error_Message": str(e)
                })
                row_has_error = True

        if not row_has_error:
            valid_rows.append(processed_row)

    # 4. REPORTING
    output_dir = os.path.join(ROOT_DIR, "data", "result")
    os.makedirs(output_dir, exist_ok=True)
    report_log = save_results(valid_rows, errors, output_dir, config)
    
    # PRINT FINAL ENGLISH SUMMARY TO CONSOLE
    print("\n" + "="*45)
    print("📊 DATA PIPELINE EXECUTION SUMMARY")
    print("="*45)
    print(f"Total Rows:     {len(df)}")
    print(f"✅ Success:     {len(valid_rows)}")
    print(f"❌ Failed:      {len(df) - len(valid_rows)}")
    print(f"⏱️ Duration:    {datetime.now() - start_time}")
    print("="*45)
    print(report_log)

if __name__ == "__main__":
    print("Starting pipeline...")
    run_pipeline()
