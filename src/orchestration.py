import os
import pandas as pd
import warnings
from datetime import datetime
from engine.loader import load_config
from engine.validators import process_datetime, check_pattern
from engine.rules import validate_business_rules
from engine.reporter import save_results

warnings.filterwarnings("ignore", category=UserWarning)

def run_pipeline():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    ROOT_DIR = os.path.dirname(BASE_DIR)
    config = load_config(os.path.join(ROOT_DIR, "mappings", "mapping.yaml"))
    excel_path = os.path.join(ROOT_DIR, "data", os.path.basename(config["source"]["file"]))
    
    df = pd.read_excel(excel_path, sheet_name=config["source"]["sheet"])
    errors, valid_rows = [], []
    seen_singles, seen_composites = {}, set()
    start_time = datetime.now()

    for index, row in df.iterrows():
        row_num = index + 2
        row_has_error = False
        processed_row = {}
        
        for col_name, rules in config["columns"].items():
            val = row.get(col_name)
            
            try:
                # 1. TYPE CONVERSION & AUTO CLEAN
                if pd.isna(val):
                    current_val = None
                elif rules.get("type") == "datetime":
                    current_val = process_datetime(val, rules)
                elif rules.get("type") == "float":
                    current_val = float(str(val).replace(',', '').strip())
                else:
                    current_val = str(val).strip() if rules.get("auto_clean") else str(val)
                    if rules.get("normalize"): current_val = current_val.lower()

                # 2. MANDATORY CHECK
                if rules.get("required") and (current_val is None or current_val == ""):
                    raise ValueError(f"Field '{col_name}' is required.")

                # 3. UNIQUENESS & COMPOSITE
                if rules.get("unique") and current_val is not None:
                    if col_name not in seen_singles: seen_singles[col_name] = set()
                    if current_val in seen_singles[col_name]:
                        raise ValueError(f"Duplicate found: {current_val}")
                    seen_singles[col_name].add(current_val)

                if "unique_composite" in rules:
                    comp_key = (current_val,) + tuple(str(row.get(c)).strip().lower() for c in rules["unique_composite"])
                    if comp_key in seen_composites:
                        raise ValueError(f"Composite duplicate: {comp_key}")
                    seen_composites.add(comp_key)

                # 4. PATTERN & BUSINESS RULES
                if "pattern" in rules and current_val is not None:
                    if not check_pattern(str(current_val), rules["pattern"]):
                        raise ValueError(f"Pattern mismatch")

                is_valid, msg = validate_business_rules(rules, current_val, row)
                if not is_valid:
                    raise ValueError(msg)

                processed_row[col_name] = current_val

            except Exception as e:
                errors.append({
                    "Row_Number": row_num, "Severity": rules.get("priority", 3),
                    "Column_Name": col_name, "Invalid_Value": val,
                    "Error_Type": "VALIDATION_FAILED", "Error_Message": str(e)
                })
                row_has_error = True

        if not row_has_error:
            valid_rows.append(processed_row)

    save_results(valid_rows, errors, os.path.join(ROOT_DIR, "data", "result"), config)
    print(f"\n✅ Execution Finished. Duration: {datetime.now() - start_time}")

if __name__ == "__main__":
    run_pipeline()
