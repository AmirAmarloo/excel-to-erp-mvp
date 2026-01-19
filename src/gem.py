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

# تعریف ترتیب اهمیت خطاها
SEVERITY_MAP = {
    "DUPLICATE_VALUE": 1,
    "REQUIRED": 2,
    "TYPE_MISMATCH": 3,
    "COLUMN_MISSING": 0,  # بحرانی‌ترین حالت
    "LIMIT_VIOLATION": 4,
    "LENGTH_VIOLATION": 5,
    "PATTERN_MISMATCH": 6
}

errors = []
valid_rows = []
seen_values = {}

excel_columns = [str(c).strip() for c in df.columns]
config_columns = config["columns"].keys()

for col in config_columns:
    if col not in excel_columns:
        errors.append({
            "Row_Number": "Global",
            "Severity": SEVERITY_MAP["COLUMN_MISSING"],
            "Column_Name": col,
            "Invalid_Value": "N/A",
            "Error_Type": "COLUMN_MISSING",
            "Error_Message": "Column not found"
        })
    if config["columns"][col].get("unique"):
        seen_values[col] = set()

if not any(e["Error_Type"] == "COLUMN_MISSING" for e in errors):
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
                    "Severity": SEVERITY_MAP["REQUIRED"],
                    "Column_Name": col_name,
                    "Invalid_Value": "NULL",
                    "Error_Type": "REQUIRED",
                    "Error_Message": "Missing mandatory value"
                })
                row_errors.append("REQUIRED")
                continue

            if not pd.isna(value):
                try:
                    if expected_type == "int":
                        current_val = int(float(value))
                    elif expected_type == "float":
                        current_val = float(value)
                    else:
                        current_val = str(value).strip()

                    if is_unique:
                        if current_val in seen_values[col_name]:
                            errors.append({
                                "Row_Number": index + 2,
                                "Severity": SEVERITY_MAP["DUPLICATE_VALUE"],
                                "Column_Name": col_name,
                                "Invalid_Value": value,
                                "Error_Type": "DUPLICATE_VALUE",
                                "Error_Message": f"Duplicate found"
                            })
                            row_errors.append("DUPLICATE")
                        else:
                            seen_values[col_name].add(current_val)

                    if "min_value" in rules and current_val < rules["min_value"]:
                        errors.append({
                            "Row_Number": index + 2,
                            "Severity": SEVERITY_MAP["LIMIT_VIOLATION"],
                            "Column_Name": col_name,
                            "Invalid_Value": value,
                            "Error_Type": "LIMIT_VIOLATION",
                            "Error_Message": "Below minimum"
                        })
                        row_errors.append("LIMIT")

                    if expected_type == "string":
                        if "min_length" in rules and len(current_val) < rules["min_length"]:
                            errors.append({"Row_Number": index + 2, "Severity": SEVERITY_MAP["LENGTH_VIOLATION"], "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "LENGTH_VIOLATION", "Error_Message": "Too short"})
                            row_errors.append("LENGTH")
                        if "max_length" in rules and len(current_val) > rules["max_length"]:
                            errors.append({"Row_Number": index + 2, "Severity": SEVERITY_MAP["LENGTH_VIOLATION"], "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "LENGTH_VIOLATION", "Error_Message": "Too long"})
                            row_errors.append("LENGTH")
                        if "pattern" in rules and not re.match(rules["pattern"], current_val):
                            errors.append({"Row_Number": index + 2, "Severity": SEVERITY_MAP["PATTERN_MISMATCH"], "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "PATTERN_MISMATCH", "Error_Message": "Invalid format"})
                            row_errors.append("PATTERN")

                    processed_row[target_name] = current_val

                except:
                    errors.append({
                        "Row_Number": index + 2,
                        "Severity": SEVERITY_MAP["TYPE_MISMATCH"],
                        "Column_Name": col_name,
                        "Invalid_Value": value,
                        "Error_Type": "TYPE_MISMATCH",
                        "Error_Message": f"Invalid {expected_type} format"
                    })
                    row_errors.append("TYPE")

        if not row_errors:
            valid_rows.append(processed_row)

# مرتب‌سازی خطاها بر اساس شدت (Severity) قبل از ذخیره
error_df = pd.DataFrame(errors)
if not error_df.empty:
    error_df = error_df.sort_values(by=["Severity", "Row_Number"])
    error_df.to_excel(f"{output_dir}/validation_errors.xlsx", index=False)

if valid_rows:
    pd.DataFrame(valid_rows).to_excel(f"{output_dir}/cleaned_data.xlsx", index=False)

print(f"Validation finished. Errors sorted by severity.")

#try:
#    subprocess.run(["git", "add", "data/result/"], check=True)
#    subprocess.run(["git", "commit", "-m", "Added error severity and sorting"], check=True)
#    subprocess.run(["git", "push"], check=True)
#except:
    pass
