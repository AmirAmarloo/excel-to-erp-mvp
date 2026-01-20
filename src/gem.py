import pandas as pd
import yaml
import os
import re
from datetime import datetime

# 1. Load configuration
with open("mappings/mapping.yaml", "r") as f:
    config = yaml.safe_load(f)

# 2. Read source Excel
df = pd.read_excel(config["source"]["file"], sheet_name=config["source"]["sheet"])
df.columns = [str(c).strip() for c in df.columns]

output_dir = "data/result"
os.makedirs(output_dir, exist_ok=True)

errors = []
valid_rows = []
seen_values = {col: set() for col, rules in config["columns"].items() if rules.get("unique")}

# 3. Processing Loop
for index, row in df.iterrows():
    row_num = index + 2
    row_has_error = False
    processed_row = {}
    
    for col_name, rules in config["columns"].items():
        value = row.get(col_name)
        target_name = rules.get("target", col_name)
        expected_type = rules["type"]
        priority = rules.get("priority", 99)

        # --- A. Auto-Cleaning ---
        if rules.get("auto_clean") and isinstance(value, str):
            value = value.strip()

        # --- B. Check Required ---
        if rules.get("required") and (pd.isna(value) or str(value).strip() == ""):
            errors.append({
                "Row_Number": row_num, "Severity": priority, "Column_Name": col_name,
                "Invalid_Value": "NULL", "Error_Type": "REQUIRED", "Error_Message": "Mandatory field"
            })
            row_has_error = True
            continue

        if not pd.isna(value):
            try:
                # --- C. Type Conversion ---
                if expected_type == "int":
                    current_val = int(float(value))
                elif expected_type == "float":
                    current_val = float(value)
                elif expected_type == "datetime":
                    current_val = pd.to_datetime(value)
                else:
                    current_val = str(value)

                # --- D. Standard Validations (Length, Range, Unique, Enum) ---
                val_str = str(current_val)
                if "min_length" in rules and len(val_str) < int(rules["min_length"]):
                    errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "LENGTH_VIOLATION", "Error_Message": "Too short"})
                    row_has_error = True
                
                if expected_type in ["int", "float"]:
                    if "min_value" in rules and float(current_val) < float(rules["min_value"]):
                        errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "LIMIT_VIOLATION", "Error_Message": "Below min"})
                        row_has_error = True

                if rules.get("unique"):
                    if val_str.lower() in seen_values[col_name]:
                        errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "DUPLICATE_VALUE", "Error_Message": "Duplicate"})
                        row_has_error = True
                    else:
                        seen_values[col_name].add(val_str.lower())

                if "allowed_values" in rules and current_val not in rules["allowed_values"]:
                    errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "INVALID_OPTION", "Error_Message": f"Options: {rules['allowed_values']}"})
                    row_has_error = True

                # --- E. Custom Logic Engine (Optimized) ---
                if "custom_rule" in rules:
                    local_env = {'value': current_val, 'row': row, 'datetime': datetime, 'pd': pd}
                    # If the rule evaluates to False, log it
                    if not eval(rules["custom_rule"], {"__builtins__": None}, local_env):
                        errors.append({
                            "Row_Number": row_num, 
                            "Severity": priority, 
                            "Column_Name": col_name, 
                            "Invalid_Value": value, 
                            "Error_Type": "CUSTOM_LOGIC_VIOLATION", 
                            "Error_Message": rules["custom_rule"] # متن قانون به عنوان پیام خطا
                        })
                        row_has_error = True

                processed_row[target_name] = current_val

            except Exception:
                errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "TYPE_MISMATCH", "Error_Message": f"Expected {expected_type}"})
                row_has_error = True

    if not row_has_error:
        valid_rows.append(processed_row)

# 4. Save Results
error_df = pd.DataFrame(errors)
if not error_df.empty:
    error_df = error_df.sort_values(by=["Severity", "Row_Number"])
    error_df.to_excel(os.path.join(output_dir, "validation_errors.xlsx"), index=False)

pd.DataFrame(valid_rows).to_excel(os.path.join(output_dir, "cleaned_data.xlsx"), index=False)

print(f"Validation finished. Errors sorted by severity.")
