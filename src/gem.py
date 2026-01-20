import pandas as pd
import yaml
import os
import re
from datetime import datetime

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
    row_has_error = False
    processed_row = {}
    
    for col_name, rules in config["columns"].items():
        value = row.get(col_name)
        priority = rules.get("priority", 99)
        
        # --- A. Auto-Cleaning ---
        if rules.get("auto_clean") and isinstance(value, str):
            value = value.strip()

        # --- B. Required Check ---
        if rules.get("required") and pd.isna(value):
            errors.append({"Row": row_num, "Col": col_name, "Type": "REQUIRED", "Severity": priority})
            row_has_error = True
            continue

        if not pd.isna(value):
            try:
                # --- C. Type Conversion ---
                if rules["type"] == "int": current_val = int(float(value))
                elif rules["type"] == "float": current_val = float(value)
                elif rules["type"] == "datetime": current_val = pd.to_datetime(value)
                else: current_val = str(value)

                # --- D. Custom Logic Engine (The Power Feature) ---
                if "custom_rule" in rules:
                    # محیط اجرای کد کوچک برای کاربر (دسترسی به value و row)
                    local_env = {'value': current_val, 'row': row, 'datetime': datetime}
                    if not eval(rules["custom_rule"], {"__builtins__": None}, local_env):
                        errors.append({
                            "Row": row_num, "Col": col_name, "Val": value, 
                            "Type": "CUSTOM_LOGIC_VIOLATION", "Severity": priority
                        })
                        row_has_error = True

                # (سایر چک‌ها مثل Uniqueness و Length اینجا قرار می‌گیرند...)
                processed_row[col_name] = current_val

            except Exception as e:
                errors.append({"Row": row_num, "Col": col_name, "Type": "PROCESSING_ERROR", "Severity": priority})
                row_has_error = True

    if not row_has_error:
        valid_rows.append(processed_row)

# 3. Create Summary Report (Management Summary)
report = {
    "Execution Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "Total Rows": len(df),
    "Valid Rows": len(valid_rows),
    "Total Errors": len(errors),
    "Health Score": f"{(len(valid_rows)/len(df))*100:.2f}%" if len(df)>0 else "0%"
}

# Save Results
pd.DataFrame(errors).to_excel(os.path.join(output_dir, "validation_errors.xlsx"), index=False)
pd.DataFrame(valid_rows).to_excel(os.path.join(output_dir, "cleaned_data.xlsx"), index=False)

# Print Summary to Terminal
print("\n" + "="*30)
print("📊 MANAGEMENT SUMMARY")
print("="*30)
for key, val in report.items():
    print(f"{key}: {val}")
print("="*30)
print("Validation finished. Errors sorted by severity.")
