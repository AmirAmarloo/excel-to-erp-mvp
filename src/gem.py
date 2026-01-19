import pandas as pd
import yaml
import os
#import subprocess
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
            "Error_Message": f"Column '{col}' not found in Excel file"
        })

if not missing_columns:
    for index, row in df.iterrows():
        row_errors = []
        processed_row = {}
        
        for col_name, rules in config["columns"].items():
            value = row.get(col_name)
            target_name = rules["target"]
            expected_type = rules["type"]
            is_required = rules.get("required", False)

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
                        if "min_value" in rules and current_val < rules["min_value"]:
                            errors.append({"Row_Number": index + 2, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "LIMIT_VIOLATION", "Error_Message": f"Value less than minimum {rules['min_value']}"})
                            row_errors.append("MIN_VALUE")
                    
                    elif expected_type == "float":
                        current_val = float(value)
                        if "min_value" in rules and current_val < rules["min_value"]:
                            errors.append({"Row_Number": index + 2, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "LIMIT_VIOLATION", "Error_Message": f"Value less than minimum {rules['min_value']}"})
                            row_errors.append("MIN_VALUE")

                    elif expected_type == "string":
                        current_val = str(value)
                        if "min_length" in rules and len(current_val) < rules["min_length"]:
                            errors.append({"Row_Number": index + 2, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "LENGTH_VIOLATION", "Error_Message": f"Length less than {rules['min_length']}"})
                            row_errors.append("MIN_LENGTH")
                        if "max_length" in rules and len(current_val) > rules["max_length"]:
                            errors.append({"Row_Number": index + 2, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "LENGTH_VIOLATION", "Error_Message": f"Length exceeds {rules['max_length']}"})
                            row_errors.append("MAX_LENGTH")
                        if "pattern" in rules:
                            if not re.match(rules["pattern"], current_val):
                                errors.append({"Row_Number": index + 2, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "PATTERN_MISMATCH", "Error_Message": "Invalid format pattern"})
                                row_errors.append("PATTERN_MISMATCH")

                    processed_row[target_name] = current_val

                except:
                    errors.append({
                        "Row_Number": index + 2,
                        "Column_Name": col_name,
                        "Invalid_Value": value,
                        "Error_Type": "TYPE_MISMATCH",
                        "Error_Message": f"Data type must be {expected_type}"
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

#try:
#    subprocess.run(["git", "add", "data/result/"], check=True)
#    subprocess.run(["git", "commit", "-m", "Validation results update - English logs"], check=True)
#    subprocess.run(["git", "push"], check=True)
#    print("Changes synced with GitHub!")
#except Exception as e:
   # print(f"Git Push failed: {e}")
