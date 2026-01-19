import pandas as pd
import yaml
import os
import re

# 1. Load configuration from YAML file
# Ensure the path matches your project structure
with open("mappings/mapping.yaml", "r") as f:
    config = yaml.safe_load(f)

# 2. Read the source Excel file
# Path and sheet name are retrieved from the YAML config
df = pd.read_excel(
    config["source"]["file"],
    sheet_name=config["source"]["sheet"]
)

# 3. Pre-processing: Clean column names by removing extra spaces
df.columns = [str(c).strip() for c in df.columns]

# 4. Setup output directory
output_dir = "data/result"
os.makedirs(output_dir, exist_ok=True)

# 5. Initialize tracking variables
errors = []
valid_rows = []
seen_values = {}

# Create a 'set' for each column that requires uniqueness
for col_name, rules in config["columns"].items():
    if rules.get("unique"):
        seen_values[col_name] = set()

# 6. Main Validation Loop: Iterate through each row of the Excel
for index, row in df.iterrows():
    row_num = index + 2  # Excel row number (header is row 1, data starts at 2)
    row_has_error = False
    processed_row = {}
    
    for col_name, rules in config["columns"].items():
        value = row.get(col_name)
        target_name = rules["target"]
        expected_type = rules["type"]
        is_required = rules.get("required", False)
        is_unique = rules.get("unique", False)
        priority = rules.get("priority", 99) # Default priority if not specified

        # --- A. Check for Required/Mandatory fields ---
        if is_required and pd.isna(value):
            errors.append({
                "Row_Number": row_num,
                "Severity": priority,
                "Column_Name": col_name,
                "Invalid_Value": "NULL",
                "Error_Type": "REQUIRED",
                "Error_Message": "This field is mandatory"
            })
            row_has_error = True
            continue

        if not pd.isna(value):
            try:
                # --- B. Data Type Conversion & Normalization ---
                if expected_type == "int":
                    current_val = int(float(value))
                elif expected_type == "float":
                    current_val = float(value)
                else:
                    current_val = str(value).strip()

                # --- C. Uniqueness Validation ---
                if is_unique:
                    # Normalize value to string and lowercase for accurate duplicate detection
                    val_to_track = str(current_val).lower() if isinstance(current_val, str) else current_val
                    if val_to_track in seen_values[col_name]:
                        errors.append({
                            "Row_Number": row_num,
                            "Severity": priority,
                            "Column_Name": col_name,
                            "Invalid_Value": value,
                            "Error_Type": "DUPLICATE_VALUE",
                            "Error_Message": f"Duplicate entry: '{current_val}' already exists"
                        })
                        row_has_error = True
                    else:
                        seen_values[col_name].add(val_to_track)

                # --- D. Numeric Range Validation ---
                if expected_type in ["int", "float"] and "min_value" in rules:
                    if current_val < rules["min_value"]:
                        errors.append({
                            "Row_Number": row_num,
                            "Severity": priority,
                            "Column_Name": col_name,
                            "Invalid_Value": value,
                            "Error_Type": "LIMIT_VIOLATION",
                            "Error_Message": f"Value is below the minimum allowed ({rules['min_value']})"
                        })
                        row_has_error = True

                # --- E. String Constraints (Length & Pattern) ---
                if expected_type == "string":
                    if "min_length" in rules and len(current_val) < rules["min_length"]:
                        errors.append({
                            "Row_Number": row_num,
                            "Severity": priority,
                            "Column_Name": col_name,
                            "Invalid_Value": value,
                            "Error_Type": "LENGTH_VIOLATION",
                            "Error_Message": "String length is too short"
                        })
                        row_has_error = True
                    
                    if "pattern" in rules and not re.match(rules["pattern"], current_val):
                        errors.append({
                            "Row_Number": row_num,
                            "Severity": priority,
                            "Column_Name": col_name,
                            "Invalid_Value": value,
                            "Error_Type": "PATTERN_MISMATCH",
                            "Error_Message": "Format does not match required pattern"
                        })
                        row_has_error = True

                # Add value to the processed data if no error found for this specific column
                processed_row[target_name] = current_val

            except Exception:
                # Type conversion failed (e.g., text found in an 'int' column)
                errors.append({
                    "Row_Number": row_num,
                    "Severity": priority,
                    "Column_Name": col_name,
                    "Invalid_Value": value,
                    "Error_Type": "TYPE_MISMATCH",
                    "Error_Message": f"Cannot convert value to {expected_type}"
                })
                row_has_error = True

    # 7. Collect Valid Rows
    # Only rows with zero errors across all columns are considered valid
    if not row_has_error:
        valid_rows.append(processed_row)

# 8. Export Results to Excel
error_df = pd.DataFrame(errors)
valid_df = pd.DataFrame(valid_rows)

# Sort errors by Priority/Severity first, then by Row Number
if not error_df.empty:
    error_df = error_df.sort_values(by=["Severity", "Row_Number"])
    error_df.to_excel(os.path.join(output_dir, "validation_errors.xlsx"), index=False)

# Export cleaned/valid rows for ERP ingestion
if not valid_df.empty:
    valid_df.to_excel(os.path.join(output_dir, "cleaned_data.xlsx"), index=False)

print(f"✅ Process finished successfully.")
print(f"   - Errors found: {len(errors)}")
print(f"   - Clean rows saved: {len(valid_rows)}")
print(f"📁 Files are ready in: {output_dir}")
