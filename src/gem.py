import pandas as pd
import yaml
import os
#import subprocess
import re

# Load configuration from YAML
with open("mappings/mapping.yaml", "r") as f:
    config = yaml.safe_load(f)

# Read source Excel file
df = pd.read_excel(
    config["source"]["file"],
    sheet_name=config["source"]["sheet"]
)

# Clean Excel column names (remove leading/trailing spaces)
df.columns = [str(c).strip() for c in df.columns]

# Ensure output directory exists
output_dir = "data/result"
os.makedirs(output_dir, exist_ok=True)

errors = []
valid_rows = []
seen_values = {}

# Initialize tracking sets for columns marked as 'unique'
for col_name, rules in config["columns"].items():
    if rules.get("unique"):
        seen_values[col_name] = set()

# Iterate through each row of the Excel file
for index, row in df.iterrows():
    row_num = index + 2  # Adjust for 1-based indexing and Excel header row
    row_has_error = False
    processed_row = {}
    
    for col_name, rules in config["columns"].items():
        value = row.get(col_name)
        target_name = rules["target"]
        expected_type = rules["type"]
        is_required = rules.get("required", False)
        is_unique = rules.get("unique", False)
        priority = rules.get("priority", 99)

        # 1. Check for missing mandatory values
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
                # 2. Type Conversion & Normalization
                if expected_type == "int":
                    current_val = int(float(value))
                elif expected_type == "float":
                    current_val = float(value)
                else:
                    current_val = str(value).strip()

                # 3. Uniqueness Validation (Case-insensitive for strings)
                if is_unique:
                    val_to_track = str(current_val).lower() if isinstance(current_val, str) else current_val
                    if val_to_track in seen_values[col_name]:
                        errors.append({
                            "Row_Number": row_num,
                            "Severity": priority,
                            "Column_Name": col_name,
                            "Invalid_Value": value,
                            "Error_Type": "DUPLICATE_VALUE",
                            "Error_Message": f"Value '{current_val}' already exists in this column"
                        })
                        row_has_error = True
                    else:
                        seen_values[col_name].add(val_to_track)

                # 4. Range Validation (for numeric types)
                if expected_type in ["int", "float"] and "min_value" in rules:
                    if current_val < rules["min_value"]:
                        errors.append({
                            "Row_Number": row_num,
                            "Severity": priority,
                            "Column_Name": col_name,
                            "Invalid_Value": value,
                            "Error_Type": "LIMIT_VIOLATION",
                            "Error_Message": f"Value below minimum ({rules['min_value']})"
                        })
                        row_has_error = True

                # 5. String Length & Pattern Validation
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

                processed_row[target_name] = current_val

            except Exception:
                # Type conversion failed
                errors.append({
                    "Row_Number": row_num,
                    "Severity": priority,
                    "Column_Name": col_name,
                    "Invalid_Value": value,
                    "Error_Type": "TYPE_MISMATCH",
                    "Error_Message": f"Failed to convert value to {expected_type}"
                })
                row_has_error = True

    # If row is clean, add to valid list
    if not row_has_error:
        valid_rows.append(processed_row)

# Create DataFrames for output
error_df = pd.DataFrame(errors)
valid_df = pd.DataFrame(valid_rows)

# Save validation errors, sorted by Severity and Row Number
if not error_df.empty:
    error_df = error_df.sort_values(by=["Severity", "Row_Number"])
    error_df.to_excel(os.path.join(output_dir, "validation_errors.xlsx"), index=False)

# Save cleaned data
if not valid_df.empty:
    valid_df.to_excel(os.path.join(output_dir, "cleaned_data.xlsx"), index=False)

print(f"✅ Process finished. Total errors found: {len(errors)}")

# Git Syncing
#try:
    #subprocess.run(["git", "add", "data/result/"], check=True)
   # subprocess.run(["git", "commit", "-m", "Auto-update: Validation results"], check=True)
  #  subprocess.run(["git", "push"], check=True)
 #   print("🚀 Git Push successful.")
#except Exception as e:
    #print(f"⚠️ Git synchronization failed: {e}")
    
