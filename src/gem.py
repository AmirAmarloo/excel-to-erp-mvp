import pandas as pd
import yaml
import os
import re

# 1. Load configuration
with open("mappings/mapping.yaml", "r") as f:
    config = yaml.safe_load(f)

df = pd.read_excel(config["source"]["file"], sheet_name=config["source"]["sheet"])
df.columns = [str(c).strip() for c in df.columns]

output_dir = "data/result"
os.makedirs(output_dir, exist_ok=True)

errors = []
valid_rows = []
seen_values = {col: set() for col, rules in config["columns"].items() if rules.get("unique")}

# 2. Validation Process
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

        # --- Length Check ---
        val_str = str(value).strip()
        val_length = len(val_str)
        if "min_length" in rules and val_length < int(rules["min_length"]):
            errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "LENGTH_VIOLATION", "Error_Message": f"Too short (min {rules['min_length']})"})
            row_has_error = True
        if "max_length" in rules and val_length > int(rules["max_length"]):
            errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Invalid_Value": f"Length: {val_length}", "Error_Type": "LENGTH_VIOLATION", "Error_Message": f"Too long (max {rules['max_length']})"})
            row_has_error = True

        try:
            # --- Step A: Convert Type ---
            if expected_type == "int":
                current_val = int(float(value))
            elif expected_type == "float":
                current_val = float(value)
            else:
                current_val = val_str

            # --- Step B: Numeric Range Check (MIN & MAX) ---
            if expected_type in ["int", "float"]:
                # Check Minimum
                if "min_value" in rules and float(current_val) < float(rules["min_value"]):
                    errors.append({
                        "Row_Number": row_num, "Severity": priority, "Column_Name": col_name,
                        "Invalid_Value": value, "Error_Type": "LIMIT_VIOLATION",
                        "Error_Message": f"Value {current_val} is below minimum {rules['min_value']}"
                    })
                    row_has_error = True
                
                # Check Maximum (This fixes your issue with 10001)
                if "max_value" in rules and float(current_val) > float(rules["max_value"]):
                    errors.append({
                        "Row_Number": row_num, "Severity": priority, "Column_Name": col_name,
                        "Invalid_Value": value, "Error_Type": "LIMIT_VIOLATION",
                        "Error_Message": f"Value {current_val} exceeds maximum {rules['max_value']}"
                    })
                    row_has_error = True

            # --- Step C: Uniqueness Check ---
            if rules.get("unique"):
                track_val = str(current_val).lower()
                if track_val in seen_values[col_name]:
                    errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "DUPLICATE_VALUE", "Error_Message": "Duplicate"})
                    row_has_error = True
                else:
                    seen_values[col_name].add(track_val)

            processed_row[target_name] = current_val

        except Exception:
            errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "TYPE_MISMATCH", "Error_Message": "Type error"})
            row_has_error = True

    if not row_has_error:
        valid_rows.append(processed_row)

# 3. Save Results
error_df = pd.DataFrame(errors)
if not error_df.empty:
    error_df = error_df.sort_values(by=["Severity", "Row_Number"])
    error_df.to_excel(os.path.join(output_dir, "validation_errors.xlsx"), index=False)

pd.DataFrame(valid_rows).to_excel(os.path.join(output_dir, "cleaned_data.xlsx"), index=False)
print(f"✅ Process finished. 10001 should now be in the error list.")
