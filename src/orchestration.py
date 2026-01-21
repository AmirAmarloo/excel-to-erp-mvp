import pandas as pd
from engine.loader import load_config, load_data
from engine.validators import process_datetime, check_pattern
from engine.rules import validate_custom_rule
from engine.reporter import save_results

# Initialize
config = load_config("mappings/mapping.yaml")
df = load_data(config)
errors = []
valid_rows = []
seen_entries = {} # For Composite Uniqueness

for index, row in df.iterrows():
    row_num = index + 2
    row_has_error = False
    processed_row = {}
    
    # Column-level validation
    for col_name, rules in config["columns"].items():
        value = row.get(col_name)
        priority = rules.get("priority", 99)
        
        # 1. Auto-Clean
        if rules.get("auto_clean") and isinstance(value, str):
            value = value.strip()

        # 2. Required Check
        if rules.get("required") and pd.isna(value):
            errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Invalid_Value": "NULL", "Error_Type": "REQUIRED", "Error_Message": "Missing data"})
            row_has_error = True
            continue

        if not pd.isna(value):
            try:
                # 3. Type Conversion
                if rules["type"] == "datetime":
                    current_val = process_datetime(value, rules)
                elif rules["type"] == "int":
                    current_val = int(float(value))
                else:
                    current_val = value

                # 4. Pattern/Regex Check
                if rules["type"] == "string" and "pattern" in rules:
                    if not check_pattern(current_val, rules["pattern"]):
                        errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "PATTERN_MISMATCH", "Error_Message": f"Regex: {rules['pattern']}"})
                        row_has_error = True

                # 5. Custom DSL Rules
                if "custom_rule" in rules:
                    if not validate_custom_rule(rules["custom_rule"], current_val, row):
                        errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "CUSTOM_LOGIC_VIOLATION", "Error_Message": str(rules["custom_rule"])})
                        row_has_error = True

                processed_row[col_name] = current_val

            except Exception as e:
                errors.append({"Row_Number": row_num, "Severity": priority, "Column_Name": col_name, "Invalid_Value": value, "Error_Type": "TYPE_MISMATCH", "Error_Message": str(e)})
                row_has_error = True

    # 6. Global Uniqueness (Composite)
    if not row_has_error:
        for col_name, rules in config["columns"].items():
            if "unique" in rules:
                u_cfg = rules["unique"]
                composite_cols = [col_name] + u_cfg.get("composite_with", [])
                key = "|".join([str(processed_row.get(c)).lower() if u_cfg.get("case_insensitive") else str(processed_row.get(c)) for c in composite_cols])
                
                scope = "-".join(composite_cols)
                if scope not in seen_entries: seen_entries[scope] = set()
                
                if key in seen_entries[scope]:
                    errors.append({"Row_Number": row_num, "Severity": rules.get("priority"), "Column_Name": col_name, "Invalid_Value": key, "Error_Type": "DUPLICATE_VALUE", "Error_Message": f"Duplicate in {scope}"})
                    row_has_error = True
                else:
                    seen_entries[scope].add(key)

    if not row_has_error:
        valid_rows.append(processed_row)

# Final Reporting
log = save_results(valid_rows, errors, "data/result", config)
print(log)
print("Validation finished. Errors sorted by severity.")
