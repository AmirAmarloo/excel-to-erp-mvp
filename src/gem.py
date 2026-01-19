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

# 2. Processing Loop
for index, row in df.iterrows():
    row_num = index + 2
    row_errors_found = False
    processed_row = {}
    
    for col_name, rules in config["columns"].items():
        value = row.get(col_name)
        priority = rules.get("priority", 99)
        expected_type = rules["type"]

        # Check Required
        if rules.get("required") and pd.isna(value):
            errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Error_Type": "REQUIRED", "Error_Message": "Field is empty"})
            row_errors_found = True
            continue

        if not pd.isna(value):
            try:
                # Type Conversion
                if expected_type == "int":
                    current_val = int(float(value))
                elif expected_type == "float":
                    current_val = float(value)
                else:
                    current_val = str(value).strip()

                # Uniqueness Check
                if rules.get("unique"):
                    val_str = str(current_val).lower()
                    if val_str in seen_values[col_name]:
                        errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Error_Type": "DUPLICATE_VALUE", "Error_Message": f"Duplicate: {current_val}"})
                        row_errors_found = True
                    else:
                        seen_values[col_name].add(val_str)

                # Length Check (Crucial for your long name)
                if expected_type == "string":
                    val_len = len(current_val)
                    if "max_length" in rules and val_len > rules["max_length"]:
                        errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Error_Type": "LENGTH_VIOLATION", "Error_Message": f"Value too long ({val_len} chars)"})
                        row_errors_found = True
                    if "min_length" in rules and val_len < rules["min_length"]:
                        errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Error_Type": "LENGTH_VIOLATION", "Error_Message": "Value too short"})
                        row_errors_found = True

                # Pattern Check
                if expected_type == "string" and "pattern" in rules:
                    if not re.match(rules["pattern"], current_val):
                        errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Error_Type": "PATTERN_MISMATCH", "Error_Message": "Invalid format"})
                        row_errors_found = True

                processed_row[rules["target"]] = current_val

            except:
                errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Error_Type": "TYPE_MISMATCH", "Error_Message": f"Should be {expected_type}"})
                row_errors_found = True

    if not row_errors_found:
        valid_rows.append(processed_row)

# 3. Save results
error_df = pd.DataFrame(errors)
if not error_df.empty:
    error_df = error_df.sort_values(by=["Severity", "Row_Number"])
    error_df.to_excel(os.path.join(output_dir, "validation_errors.xlsx"), index=False)

pd.DataFrame(valid_rows).to_excel(os.path.join(output_dir, "cleaned_data.xlsx"), index=False)

print(f"Validation complete. Found {len(errors)} error entries.")
