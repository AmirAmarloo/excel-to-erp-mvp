import pandas as pd
import yaml
import os
#import subprocess
import re

with open("config/mapping.yaml", "r") as f:
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
            "Error_Type": "COLUMN_MISSING",
            "Error_Message": f"The '{col}' was not found."
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
                    "Error_Type": "REQUIRED",
                    "Error_Message": "The column has no value."
                })
                row_errors.append("REQUIRED")
                continue

            if not pd.isna(value):
                try:
                    if expected_type == "int":
                        value = int(value)
                        if "min_value" in rules and value < rules["min_value"]:
                            row_errors.append("MIN_VALUE")
                            errors.append({"Row_Number": index + 2, "Column_Name": col_name, "Error_Type": "LIMIT_VIOLATION", "Error_Message": f"Less than the allowed limit {rules['min_value']}"})
                    
                    elif expected_type == "float":
                        value = float(value)
                        if "min_value" in rules and value < rules["min_value"]:
                            row_errors.append("MIN_VALUE")
                            errors.append({"Row_Number": index + 2, "Column_Name": col_name, "Error_Type": "LIMIT_VIOLATION", "Error_Message": f"More than the allowed limit {rules['min_value']}"})

                    elif expected_type == "string":
                        value = str(value)
                        if "min_length" in rules and len(value) < rules["min_length"]:
                            row_errors.append("MIN_LENGTH")
                            errors.append({"Row_Number": index + 2, "Column_Name": col_name, "Error_Type": "LENGTH_VIOLATION", "Error_Message": f"The length is less than from {rules['min_length']}"})
                        if "max_length" in rules and len(value) > rules["max_length"]:
                            row_errors.append("MAX_LENGTH")
                            errors.append({"Row_Number": index + 2, "Column_Name": col_name, "Error_Type": "LENGTH_VIOLATION", "Error_Message": f"The length is more than from {rules['max_length']}"})
                        if "pattern" in rules and not re.match(rules["pattern"], value):
                            row_errors.append("PATTERN_MISMATCH")
                            errors.append({"Row_Number": index + 2, "Column_Name": col_name, "Error_Type": "FORMAT_VIOLATION", "Error_Message": "فرمت مقدار ناصحیح است"})

                except:
                    errors.append({
                        "Row_Number": index + 2,
                        "Column_Name": col_name,
                        "Error_Type": "TYPE_MISMATCH",
                        "Error_Message": f"The value should be {expected_type} "
                    })
                    row_errors.append("TYPE_MISMATCH")
                    continue

            processed_row[target_name] = value

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
#    subprocess.run(["git", "commit", "-m", "Advanced validation added"], check=True)
#    subprocess.run(["git", "push"], check=True)
#    print("Changes synced with GitHub!")
#except Exception as e:
   # print(f"Git Push failed: {e}")
