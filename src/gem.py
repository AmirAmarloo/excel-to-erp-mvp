import pandas as pd
import yaml
import os
import re

# 1. Load configuration
with open("mappings/mapping.yaml", "r") as f:
    config = yaml.safe_load(f)

# 2. Read Excel
df = pd.read_excel(config["source"]["file"], sheet_name=config["source"]["sheet"])
df.columns = [str(c).strip() for c in df.columns]

output_dir = "data/result"
os.makedirs(output_dir, exist_ok=True)

errors = []
valid_rows = []
seen_values = {col: set() for col, rules in config["columns"].items() if rules.get("unique")}

# 3. Validation Process
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
        if rules.get("required") and pd.isna(value):
            errors.append({
                "Row_Number": row_num, "Severity": priority, "Column_Name": col_name,
                "Invalid_Value": "NULL", "Error_Type": "REQUIRED", "Error_Message": "Mandatory field"
            })
            row_has_error = True
            continue

        if not pd.isna(value):
            try:
                # --- Step A: Convert Type ---
                if expected_type == "int":
                    current_val = int(float(value))
                elif expected_type == "float":
                    current_val = float(value)
                else:
                    current_val = str(value).strip()

                # --- Step B: Range Check (This is where 101 should fail) ---
                # Check min_value for ANY numeric column
                if "min_value" in rules:
                    if float(current_val) < float(rules["min_value"]):
                        errors.append({
                            "Row_Number": row_num, "Severity": priority, "Column_Name": col_name,
                            "Invalid_Value": value, "Error_Type": "LIMIT_VIOLATION",
                            "Error_Message": f"Value {current_val} is below minimum {rules['min_value']}"
                        })
                        row_has_error = True

                # --- Step C: Uniqueness Check ---
                if rules.get("unique"):
                    val_str = str(current_val).lower()
                    if val_str in seen_values[col_name]:
                        errors.append({
                            "Row_Number": row_num, "Severity": priority, "Column_Name": col_name,
                            "Invalid_Value": value, "Error_Type": "DUPLICATE_VALUE", "Error_Message": "Duplicate"
                        })
                        row_has_error = True
                    else:
                        seen_values[col_name].add(val_str)

                # --- Step D: Length Check ---
                if expected_type == "string":
                    if "max_length" in rules and len(current_val) > rules["max_length"]:
                        errors.append({
                            "Row_Number": row_num, "Severity": priority, "Column_Name": col_name,
                            "Invalid_Value": f"Length: {len(current_val)}", "Error_Type": "LENGTH_VIOLATION",
                            "Error_Message": f"Max allowed is {rules['max_length']}"
                        })
                        row_has_error = True

                processed_row[target_name] = current_val

            except Exception as e:
                errors.append({
                    "Row_Number": row_num, "Severity": priority, "Column_Name": col_name,
                    "Invalid_Value": value, "Error_Type": "TYPE_MISMATCH", "Error_Message": "Type error"
                })
                row_has_error = True

    if not row_has_error:
        valid_rows.append(processed_row)

# 4. Save results
error_df = pd.DataFrame(errors)
if not error_df.empty:
    error_df = error_df.sort_values(by=["Severity", "Row_Number"])
    error_df.to_excel(os.path.join(output_dir, "validation_errors.xlsx"), index=False)

pd.DataFrame(valid_rows).to_excel(os.path.join(output_dir, "cleaned_data.xlsx"), index=False)
print(f"Validation finished. Total errors: {len(errors)}")
