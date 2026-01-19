import pandas as pd
import yaml
import os
import subprocess
import re

with open("mappings/mapping.yaml", "r") as f:
    config = yaml.safe_load(f)

df = pd.read_excel(
    config["source"]["file"],
    sheet_name=config["source"]["sheet"]
)

output_dir = "data/result"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

errors = []
valid_rows = []
seen_values = {}

excel_columns = [str(c).strip() for c in df.columns]
config_columns = config["columns"].keys()
missing_columns = []

for col in config_columns:
    if col not in excel_columns:
        missing_columns.append(col)
        errors.append({
            "Row_Number": "Global",
            "Column_Name": col,
            "Invalid_Value": "N/A",
            "Error_Type": "COLUMN_MISSING",
            "Error_Message": f"Column '{col}' not found"
        })
    
    col_rules = config["columns"][col]
    if col_rules.get("unique") is True:
        seen_values[col] = set()

if not missing_columns:
    for index, row in df.iterrows():
        row_errors = []
        processed_row = {}
        
        for col_name, rules in config["columns"].items():
            value = row.get(col_name)
            target_name = rules["target"]
            expected_type = rules["type"]
            is_required = rules.get("required", False)
            is_unique = rules.get("unique", False)

            if is_required and pd.isna(value):
                errors.append({
                    "Row_Number": index + 2,
                    "Column_Name": col_name,
                    "Invalid_Value": "NULL",
                    "Error_Type": "REQUIRED",
                    "Error_Message": "Value is mandatory but missing"
                })
                row_errors.append("REQUIRED")
                continue

            if not pd.isna(value):
                try:
                    current_val = value
                    if expected_type == "int":
                        current_val = int(value)
                    elif expected_type == "float":
                        current_val = float(value)
                    elif expected_type == "string":
                        current_val = str(value).strip()

                    if is_unique:
                        if current_val in seen_values[col_name]:
                            errors.append({
                                "Row_Number": index + 2,
                                "Column_Name": col_name,
                                "Invalid_Value": value,
                                "Error_Type": "DUPLICATE_VALUE",
                                "Error_Message": f"Duplicate value detected"
                            })
                            row_errors.append("DUPLICATE")
                        else:
                            seen_values[col_name].add(current_val)

                    if "min_value" in rules and current_val < rules["min_value"]:
                        errors.append({"Row_Number": index + 2, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "LIMIT_VIOLATION", "Error_Message": f"Value less than {rules['min_value']}"})
                        row_errors.append("MIN_VALUE")
                    
                    if expected_type == "string":
                        if "min_length" in rules and len(current_val) < rules["min_length"]:
                            errors.append({"Row_Number": index + 2, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "LENGTH_VIOLATION", "Error_Message": "Too short"})
                            row_errors.append("MIN_LENGTH")
                        if "max_length" in rules and len(current_val) > rules["max_length"]:
                            errors.append({"Row_Number": index + 2, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "LENGTH_VIOLATION", "Error_Message": "Too long"})
                            row_errors.append("MAX_LENGTH")
                        if "pattern" in rules and not re.match(rules["pattern"], current_val):
                            errors.append({"Row_Number": index + 2, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "PATTERN_MISMATCH", "Error_Message": "Invalid format"})
                            row_errors.append("PATTERN_MISMATCH")

                    processed_row[target_name] = current_val

                except:
                    errors.append({
                        "Row_Number": index + 2,
                        "Column_Name": col_name,
                        "Invalid_Value": value,
                        "Error_Type": "TYPE_MISMATCH",
                        "Error_Message": f"Must be {expected_type}"
                    })
                    row_errors.append("TYPE_MISMATCH")

        if not row_errors:
            valid_rows.append(processed_row)

output_df = pd.DataFrame(valid_rows)
error_df = pd.DataFrame(errors)

if not error_df.empty:
    error_df.to_excel(f"{output_dir}/validation_errors.xlsx", index=False)
if not output_df.empty:
    output_df.to_excel(f"{output_dir}/cleaned_data.xlsx", index=False)

print(f"Validation complete. Total errors: {len(errors)}")

#try:
 #   subprocess.run(["git", "add", "data/result/"], check=True)
 #   subprocess.run(["git", "commit", "-m", "Updated path and fixed uniqueness logic"], check=True)
 #   subprocess.run(["git", "push"], check=True)
#except:
    #pass
