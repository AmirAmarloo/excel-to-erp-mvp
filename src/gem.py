import pandas as pd
import yaml
import os
import re

# 1. Load config
with open("mappings/mapping.yaml", "r") as f:
    config = yaml.safe_load(f)

df = pd.read_excel(config["source"]["file"], sheet_name=config["source"]["sheet"])
df.columns = [str(c).strip() for c in df.columns]

output_dir = "data/result"
os.makedirs(output_dir, exist_ok=True)

errors = []
valid_rows = []
seen_values = {col: set() for col, rules in config["columns"].items() if rules.get("unique")}

# 2. Validation Loop
for index, row in df.iterrows():
    row_num = index + 2
    row_has_error = False
    processed_row = {}
    
    for col_name, rules in config["columns"].items():
        value = row.get(col_name)
        target_name = rules["target"]
        expected_type = rules["type"]
        priority = rules.get("priority", 99)

        # --- Check Required ---
        if rules.get("required") and (pd.isna(value) or str(value).strip() == ""):
            errors.append({
                "Row_Number": row_num, "Severity": priority, "Column_Name": col_name,
                "Invalid_Value": "NULL", "Error_Type": "REQUIRED", "Error_Message": "Mandatory field"
            })
            row_has_error = True
            continue

        # --- Length Check (STRIP SPACES FIRST) ---
        # We do this before type conversion to catch "A" even if it's stored as an object
        val_str = str(value).strip()
        val_length = len(val_str)

        if "min_length" in rules and val_length < int(rules["min_length"]):
            errors.append({
                "Row_Number": row_num, "Severity": priority, "Column_Name": col_name,
                "Invalid_Value": value, "Error_Type": "LENGTH_VIOLATION",
                "Error_Message": f"Value '{val_str}' is too short (Length: {val_length}, Min required: {rules['min_length']})"
            })
            row_has_error = True
            # We don't 'continue' here because we want to catch other errors too

        if "max_length" in rules and val_length > int(rules["max_length"]):
            errors.append({
                "Row_Number": row_num, "Severity": priority, "Column_Name": col_name,
                "Invalid_Value": f"Length: {val_length}", "Error_Type": "LENGTH_VIOLATION",
                "Error_Message": f"Value exceeds max length of {rules['max_length']}"
            })
            row_has_error = True

        try:
            # --- Type Conversion ---
            if expected_type == "int":
                current_val = int(float(value))
            elif expected_type == "float":
                current_val = float(value)
            else:
                current_val = val_str

            # --- Range Check ---
            if "min_value" in rules and float(current_val) < float(rules["min_value"]):
                errors.append({
                    "Row_Number": row_num, "Severity": priority, "Column_Name": col_name,
                    "Invalid_Value": value, "Error_Type": "LIMIT_VIOLATION",
                    "Error_Message": f"Below minimum {rules['min_value']}"
                })
                row_has_error = True

            # --- Uniqueness Check ---
            if rules.get("unique"):
                track_val = str(current_val).lower()
                if track_val in seen_values[col_name]:
                    errors.append({
                        "Row_Number": row_num, "Severity": priority, "Column_Name": col_name,
                        "Invalid_Value": value, "Error_Type": "DUPLICATE_VALUE", "Error_Message": "Duplicate entry"
                    })
                    row_has_error = True
                else:
                    seen_values[col_name].add(track_val)

            processed_row[target_name] = current_val

        except Exception:
            errors.append({
                "Row_Number": row_num, "Severity": priority, "Column_Name": col_name,
                "Invalid_Value": value, "Error_Type": "TYPE_MISMATCH", "Error_Message": "Type mismatch"
            })
            row_has_error = True

    if not row_has_error:
        valid_rows.append(processed_row)

# 3. Save to Excel
error_df = pd.DataFrame(errors)
if not error_df.empty:
    error_df = error_df.sort_values(by=["Severity", "Row_Number"])
    error_df.to_excel(os.path.join(output_dir, "validation_errors.xlsx"), index=False)

pd.DataFrame(valid_rows).to_excel(os.path.join(output_dir, "cleaned_data.xlsx"), index=False)
print(f"Validation Finished. Errors: {len(errors)}")
