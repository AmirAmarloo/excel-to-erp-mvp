import pandas as pd
import yaml
import os
#import subprocess
import re
import sys

try:
    # ۱. تست خواندن فایل تنظیمات
    with open("mappings/mapping.yaml", "r") as f:
        config = yaml.safe_load(f)
    print("✅ YAML file loaded successfully.")

    # ۲. تست خواندن فایل اکسل
    file_path = config["source"]["file"]
    sheet = config["source"]["sheet"]
    df = pd.read_excel(file_path, sheet_name=sheet)
    print(f"✅ Excel file '{file_path}' loaded successfully.")

    # ۳. بررسی ستون مورد نظر
    col_to_check = "customr_id" 
    
    print(f"\n--- Debug Info ---")
    print(f"1. Actual Columns in Excel: {list(df.columns)}")
    
    # پاکسازی نام ستون‌ها (حذف فاصله‌های احتمالی)
    df.columns = [str(c).strip() for c in df.columns]
    
    if col_priority_check := col_to_check in df.columns:
        print(f"2. Is '{col_to_check}' found? Yes")
        print(f"3. Duplicates count in this column: {df[col_to_check].duplicated().sum()}")
        print(f"4. Sample values: {df[col_to_check].head(5).tolist()}")
    else:
        print(f"2. Is '{col_to_check}' found? NO (Column name mismatch)")
    print(f"------------------\n")

except Exception as e:
    print(f"❌ Error occurred: {e}")

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

for col in config_columns:
    if col not in excel_columns:
        errors.append({
            "Row_Number": "Global",
            "Severity": 0,
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
            col_priority = rules.get("priority", 99)

            if is_required and pd.isna(value):
                errors.append({
                    "Row_Number": index + 2,
                    "Severity": col_priority,
                    "Column_Name": col_name,
                    "Invalid_Value": "NULL",
                    "Error_Type": "REQUIRED",
                    "Error_Message": "Missing mandatory value"
                })
                row_errors.append("REQ")
                continue

            if not pd.isna(value):
                try:
                    # گام اول: تبدیل به نوع داده هدف
                    if expected_type == "int":
                        current_val = int(float(value))
                    elif expected_type == "float":
                        current_val = float(value)
                    else:
                        current_val = str(value).strip()

                    # گام دوم: چک کردن تکراری بودن (تبدیل به String برای مقایسه دقیق)
                    if is_unique:
                        val_str = str(current_val) # یکسان‌سازی نهایی
                        if val_str in seen_values[col_name]:
                            errors.append({
                                "Row_Number": index + 2,
                                "Severity": col_priority,
                                "Column_Name": col_name,
                                "Invalid_Value": value,
                                "Error_Type": "DUPLICATE_VALUE",
                                "Error_Message": f"Duplicate value '{current_val}' detected"
                            })
                            row_errors.append("DUP")
                        else:
                            seen_values[col_name].add(val_str)

                    # گام سوم: سایر اعتبارسنجی‌ها
                    if expected_type in ["int", "float"] and "min_value" in rules:
                        if current_val < rules["min_value"]:
                            errors.append({"Row_Number": index + 2, "Severity": col_priority, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "LIMIT_VIOLATION", "Error_Message": "Below minimum"})
                            row_errors.append("LIM")

                    if expected_type == "string":
                        if "min_length" in rules and len(current_val) < rules["min_length"]:
                            errors.append({"Row_Number": index + 2, "Severity": col_priority, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "LENGTH_VIOLATION", "Error_Message": "Too short"})
                            row_errors.append("LEN")
                        if "pattern" in rules and not re.match(rules["pattern"], current_val):
                            errors.append({"Row_Number": index + 2, "Severity": col_priority, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "PATTERN_MISMATCH", "Error_Message": "Invalid format"})
                            row_errors.append("PAT")

                    processed_row[target_name] = current_val

                except:
                    errors.append({
                        "Row_Number": index + 2,
                        "Severity": col_priority,
                        "Column_Name": col_name,
                        "Invalid_Value": value,
                        "Error_Type": "TYPE_MISMATCH",
                        "Error_Message": "Type conversion error"
                    })
                    row_errors.append("TYPE")

        if not row_errors:
            valid_rows.append(processed_row)

error_df = pd.DataFrame(errors)
if not error_df.empty:
    error_df = error_df.sort_values(by=["Severity", "Row_Number"])
    error_df.to_excel(f"{output_dir}/validation_errors.xlsx", index=False)

if valid_rows:
    pd.DataFrame(valid_rows).to_excel(f"{output_dir}/cleaned_data.xlsx", index=False)

print(f"Validation complete. Total errors: {len(errors)}")

#try:
#    subprocess.run(["git", "add", "data/result/"], check=True)
#    subprocess.run(["git", "commit", "-m", "Fixed uniqueness detection by string normalization"], check=True)
#    subprocess.run(["git", "push"], check=True)
#except:
    #pass
