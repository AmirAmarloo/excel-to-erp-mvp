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
start_time = datetime.now()
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
                # --- C. Type Conversion (Fixed dayfirst Warning) ---
                if expected_type == "int":
                    current_val = int(float(value))
                elif expected_type == "float":
                    current_val = float(value)
                elif expected_type == "datetime":
                    current_val = pd.to_datetime(value, dayfirst=True)
                else:
                    current_val = str(value)

                # --- D. Standard Validations ---
                val_str = str(current_val)
                if "min_length" in rules and len(val_str) < int(rules["min_length"]):
                    errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "LENGTH_VIOLATION", "Error_Message": "Too short"})
                    row_has_error = True
                
                if expected_type in ["int", "float"]:
                    if "min_value" in rules and float(current_val) < float(rules["min_value"]):
                        errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "LIMIT_VIOLATION", "Error_Message": "Below min"})
                        row_has_error = True
                    if "max_value" in rules and float(current_val) > float(rules["max_value"]):
                        errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "LIMIT_VIOLATION", "Error_Message": "Exceeds max"})
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

                # --- E. Custom Logic Engine ---
                if "custom_rule" in rules:
                    local_env = {'value': current_val, 'row': row, 'datetime': datetime, 'pd': pd}
                    if not eval(rules["custom_rule"], {"__builtins__": None}, local_env):
                        errors.append({
                            "Row_Number": row_num, "Severity": priority, "Column_Name": col_name, 
                            "Invalid_Value": value, "Error_Type": "CUSTOM_LOGIC_VIOLATION", 
                            "Error_Message": rules["custom_rule"]
                        })
                        row_has_error = True

                processed_row[target_name] = current_val

            except Exception:
                errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "TYPE_MISMATCH", "Error_Message": f"Expected {expected_type}"})
                row_has_error = True

    if not row_has_error:
        valid_rows.append(processed_row)

# 4. Save Results and Create Log File
pd.DataFrame(valid_rows).to_excel(os.path.join(output_dir, "cleaned_data.xlsx"), index=False)
error_df = pd.DataFrame(errors)
if not error_df.empty:
    error_df = error_df.sort_values(by=["Severity", "Row_Number"])
    error_df.to_excel(os.path.join(output_dir, "validation_errors.xlsx"), index=False)

# --- F. Summary and Logging ---
health_score = (len(valid_rows) / len(df)) * 100 if len(df) > 0 else 0
log_content = f"""
========================================
📊 EXECUTION SUMMARY REPORT
========================================
Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Source File: {config['source']['file']}
Total Rows Processed: {len(df)}
----------------------------------------
✅ Valid Rows: {len(valid_rows)}
❌ Rows with Errors: {len(df) - len(valid_rows)}
⚠️ Total Error Entries: {len(errors)}
📈 Data Health Score: {health_score:.2f}%
========================================
"""

# Save log to file
with open(os.path.join(output_dir, "execution_summary.txt"), "w", encoding="utf-8") as f:
    f.write(log_content)

print(log_content)
print(f"Validation finished. Errors sorted by severity.")
