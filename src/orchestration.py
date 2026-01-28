import warnings
from datetime import datetime
import pandas as pd
from engine.loader import load_config, load_source_chunks, get_result_path
from engine.validators import process_datetime, check_pattern, DataValidationError, ErrorType
from engine.rules import validate_business_rules
from engine.reporter import save_results

# Suppress spreadsheet engine warnings
warnings.filterwarnings("ignore", category=UserWarning)

def run_pipeline(chunk_size=50000):
    """
    ULTIMATE STRUCTURED ORCHESTRATOR:
    Processes massive datasets in chunks while maintaining data integrity,
    global uniqueness (3-column composite), and machine-readable error logs.
    """
    try:
        config = load_config()
    except Exception as e:
        print(f"PIPELINE CRITICAL FAILURE: {e}")
        return

    errors, valid_rows = [], []
    # Global sets to track uniqueness across all chunks
    seen_singles, seen_composites = {}, set()
    start_time = datetime.now()
    global_row_counter = 2 # Starting from Excel row 2 (row 1 is header)

    # 1. PROCESS DATA IN CHUNKS
    for chunk_df in load_source_chunks(config, chunk_size=chunk_size):
        for index, row in chunk_df.iterrows():
            row_has_error = False
            processed_row = {}

            for col_name, rules in config["columns"].items():
                val = row.get(col_name)

                try:
                    # --- A. CLEANING & NORMALIZATION ---
                    if rules.get("type") == "datetime":
                        current_val = process_datetime(val, rules)
                    elif rules.get("type") == "float":
                        try:
                            # Remove commas and handle NaN/None
                            current_val = float(str(val).replace(',', '').strip()) if pd.notna(val) else None
                        except:
                            raise DataValidationError(ErrorType.TYPE_MISMATCH, "Invalid float format")
                    else:
                        # Auto-clean and Lowercase normalization
                        current_val = str(val).strip() if rules.get("auto_clean") and pd.notna(val) else val
                        if rules.get("normalize") and current_val:
                            current_val = current_val.lower()

                    # --- B. MANDATORY CHECK ---
                    if rules.get("required") and (current_val is None or str(current_val).strip() == ""):
                        raise DataValidationError(ErrorType.MISSING_REQUIRED, f"Field {col_name} is mandatory.")

                    # --- C. SINGLE COLUMN UNIQUENESS ---
                    if rules.get("unique") and current_val is not None:
                        if col_name not in seen_singles: seen_singles[col_name] = set()
                        if current_val in seen_singles[col_name]:
                            raise DataValidationError(ErrorType.DUPLICATE_ENTRY, f"Duplicate value: {current_val}")
                        seen_singles[col_name].add(current_val)

                    # --- D. COMPOSITE UNIQUENESS GUARD (Multi-column Uniqueness) ---
                    if "unique_composite" in rules:
                        # Assemble the key values
                        comp_members = [current_val] + [row.get(c) for c in rules["unique_composite"]]
                        
                        # Guard: Ensure all members of the composite key are valid (No NaN/None)
                        if any(pd.isna(v) or str(v).strip().lower() in ("none", "nan", "") for v in comp_members):
                            raise DataValidationError(ErrorType.BUSINESS_RULE_VIOLATION, "Incomplete Composite Key")
                        
                        # Normalize and hash the key
                        comp_key = tuple(str(v).strip().lower() for v in comp_members)
                        if comp_key in seen_composites:
                            raise DataValidationError(ErrorType.COMPOSITE_DUPLICATE, f"Composite Duplicate found: {comp_key}")
                        seen_composites.add(comp_key)

                    # --- E. PATTERN & BUSINESS RULES ---
                    if "pattern" in rules and current_val is not None:
                        if not check_pattern(str(current_val), rules["pattern"]):
                            raise DataValidationError(ErrorType.PATTERN_MISMATCH, f"Format mismatch for {col_name}")

                    # Validate complex rules (e.g., Suspend logic)
                    is_valid, msg = validate_business_rules(rules, current_val, row)
                    if not is_valid:
                        raise DataValidationError(ErrorType.BUSINESS_RULE_VIOLATION, msg)

                    processed_row[col_name] = current_val

                except DataValidationError as e:
                    # Capture structured error for system/ERP integration
                    errors.append({
                        "Row_Number": global_row_counter,
                        "Column_Name": col_name,
                        "Error_Type": e.error_type,
                        "Severity": rules.get("priority", 2),
                        "Error_Message": e.message
                    })
                    row_has_error = True
                except Exception as e:
                    # Capture unexpected system-level errors
                    errors.append({
                        "Row_Number": global_row_counter,
                        "Column_Name": col_name,
                        "Error_Type": ErrorType.SYSTEM_CRASH,
                        "Severity": 1,
                        "Error_Message": str(e)
                    })
                    row_has_error = True

            if not row_has_error:
                valid_rows.append(processed_row)
            
            global_row_counter += 1

    # 2. FINAL EXPORT AND REPORTING
    result_dir = get_result_path(config)
    summary = save_results(valid_rows, errors, result_dir, config)

    print("\n" + "="*55)
    print(f"✅ PIPELINE SUCCESS: {len(valid_rows)} Valid Rows")
    print(f"❌ PIPELINE ERRORS:  {len(errors)} Issues Found")
    print(f"🕒 TOTAL DURATION:   {datetime.now() - start_time}")
    print("="*55)
    print(summary)

if __name__ == "__main__":
    run_pipeline()