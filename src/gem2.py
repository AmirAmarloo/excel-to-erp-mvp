import pandas as pd
import yaml
import os

with open("config/mapping.yaml", "r") as f:
    config = yaml.safe_load(f)

df = pd.read_excel(
    config["source"]["file"],
    sheet_name=config["source"]["sheet"]
)

errors = []
valid_rows = []

for index, row in df.iterrows():
    row_errors = []
    processed_row = {}
    
    for col_name, rules in config["columns"].items():
        value = row.get(col_name)
        target_name = rules["target"]
        expected_type = rules["type"]
        is_required = rules["required"]

        if is_required and pd.isna(value):
            row_errors.append(f"Field {col_name} is required")
            continue

        if not pd.isna(value):
            try:
                if expected_type == "int":
                    value = int(value)
                elif expected_type == "float":
                    value = float(value)
                elif expected_type == "string":
                    value = str(value)
            except:
                row_errors.append(f"Type mismatch in {col_name}")
                continue

        processed_row[target_name] = value

    if row_errors:
        error_record = row.to_dict()
        error_record["Row_Number"] = index + 2
        error_record["Error_Message"] = " | ".join(row_errors)
        errors.append(error_record)
    else:
        valid_rows.append(processed_row)

output_df = pd.DataFrame(valid_rows)
error_df = pd.DataFrame(errors)

if not error_df.empty:
    error_df.to_excel("validation_errors.xlsx", index=False)

if not output_df.empty:
    output_df.to_excel("cleaned_data.xlsx", index=False)

print("Process finished.")
