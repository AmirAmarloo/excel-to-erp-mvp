import os
import pandas as pd
import warnings
from datetime import datetime
from engine.loader import load_config
from engine.validators import process_datetime, check_pattern
from engine.rules import validate_custom_rule
from engine.reporter import save_results

# Suppress library warnings for a cleaner console output
warnings.filterwarnings("ignore", category=UserWarning)

def run_pipeline():
    """
    MAIN ORCHESTRATOR: Handles Type Casting, Uniqueness, 
    Business Rules, and generates English reports.
    """
    # 1. PATH RESOLUTION
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    ROOT_DIR = os.path.dirname(BASE_DIR)
    
    config_path = os.path.join(ROOT_DIR, "mappings", "mapping.yaml")
    config = load_config(config_path)
    
    excel_filename = os.path.basename(config["source"]["file"])
    excel_path = os.path.join(ROOT_DIR, "data", excel_filename)
    
    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"Source file not found: {excel_path}")

    # 2. DATA LOADING
    df = pd.read_excel(excel_path, sheet_name=config["source"]["sheet"])
    
    errors = []
    valid_rows = []
    seen_values = {} # To track unique constraints
    start_time = datetime.now()

    # 3. PROCESSING LOOP
    for index, row in df.iterrows():
        row_num = index + 2
        row_has_error = False
        processed_row = {}
        
        for col_name, rules in config["columns"].items():
            val = row.get(col_name)
            col_type = rules.get("type")
            priority = rules.get("priority", 3)
            
            try:
                # --- A. DATA TYPE VALIDATION ---
                if pd.isna(val) or str(val).strip() == "":
                    current_val = None
                elif col_type == "datetime":
                    current_val = process_datetime(val, rules)
                elif col_type == "float":
                    try:
                        current_val = float(str(val).replace(',', '').strip())
                    except ValueError:
                        raise TypeError(f"Invalid numeric format: {val}")
                elif col_type == "int":
                    try:
                        current_val = int(float(str(val).strip()))
                    except ValueError:
                        raise TypeError(f"Invalid integer format: {val}")
                else:
                    current_val = str(val).strip()

                # --- B. MANDATORY CONSTRAINT ---
                if rules.get("required") and current_val is None:
                    raise ValueError(f"Required field '{col_name}' is empty.")

                # --- C. UNIQUENESS CONSTRAINT ---
                if rules.get("unique") and current_val is not None:
                    if col_name not in seen_values: seen_values[col_name] = set()
                    if current_val in seen_values[col_name]:
                        raise ValueError(f"Duplicate value detected: {current_val}")
                    seen_values[col_name].add(current_val)

                # --- D. BUSINESS LOGIC (Length, Suspend, Ranges) ---
                if "custom_rule" in rules and current_val is not None:
                    if not validate_custom_rule(rules["custom_rule"], current_val, row):
                        rule_type = rules["custom_rule"]["type"]
                        raise ValueError(f"Business Rule Violation: {rule_type}")

                processed_row[col_name] = current_val

            except (TypeError, ValueError) as e:
                errors.append({
                    "Row_Number": row_num, 
                    "Severity": priority,
                    "Column_Name": col_name, 
                    "Invalid_Value": val,
                    "Error_Type": "TYPE_OR_VALIDATION_ERROR", 
                    "Error_Message": str(e)
                })
                row_has_error = True

        if not row_has_error:
            valid_rows.append(processed_row)

    # 4. REPORTING & EXPORT
    output_dir = os.path.join(ROOT_DIR, "data", "result")
    os.makedirs(output_dir, exist_ok=True)
    
    # Save output files (CSV/XLSX)
    report_summary = save_results(valid_rows, errors, output_dir, config)
    
    # Final Console Statistics (All English)
    print("\n" + "="*45)
    print("📊 DATA PIPELINE EXECUTION SUMMARY")
    print("="*45)
    print(f"Total Records:      {len(df)}")
    print(f"✅ Valid Records:   {len(valid_rows)}")
    print(f"❌ Failed Records:  {len(df) - len(valid_rows)}")
    print(f"⏱️  Duration:        {datetime.now() - start_time}")
    print(f"📁 Output Folder:   {output_dir}")
    print("="*45)
    print(report_summary) 

if __name__ == "__main__":
    try:
        print("Starting Data Validation Pipeline...")
        run_pipeline()
        print("Pipeline Execution Finished Successfully.")
    except Exception as e:
        print(f"CRITICAL SYSTEM FAILURE: {e}")
