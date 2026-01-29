import warnings
from datetime import datetime
import pandas as pd
from engine.loader import load_config, load_source_chunks, get_result_path
from engine.validators import process_datetime, check_pattern, DataValidationError, ErrorType
from engine.rules import validate_business_rules
from engine.reporter import save_results

# Suppress spreadsheet engine warnings for a cleaner console output
warnings.filterwarnings("ignore", category=UserWarning)

def run_pipeline(chunk_size=50000):
    """
    CORE ORCHESTRATOR (Commercial Version)
    - Processes data in chunks.
    - Prevents partial or erroneous rows from entering the clean dataset.
    - Preserves invalid values in the error report for debugging.
    - Robust against NaN/Null values.
    """
    try:
        config = load_config()
    except Exception as e:
        print(f"CRITICAL: Could not load configuration: {e}")
        return

    errors, valid_rows = [], []
    
    # Global state for uniqueness tracking across all chunks
    seen_singles = {} 
    seen_composites = set()
    
    start_time = datetime.now()
    global_row_counter = 2  # Excel row numbering (assuming Row 1 is header)

    # 1. PROCESSING DATA IN CHUNKS
    for chunk_df in load_source_chunks(config, chunk_size=chunk_size):
        for index, row in chunk_df.iterrows():
            row_errors = []
            temp_processed_row = {}
            
            # We first collect all column data for the row
            for col_name, rules in config["columns"].items():
                original_val = row.get(col_name)
                
                try:
                    # --- A. INITIAL CLEANING & TYPE HANDLING ---
                    current_val = original_val
                    
                    # Prevent 'float' has no attribute 'lower' errors (NaN handling)
                    is_null = pd.isna(original_val) or str(original_val).strip() == ""
                    
                    if not is_null:
                        if rules.get("auto_clean"):
                            current_val = str(original_val).strip()
                        if rules.get("normalize"):
                            current_val = str(current_val).lower()

                    # --- B. MANDATORY CHECK ---
                    if rules.get("required") and is_null:
                        raise DataValidationError(ErrorType.MISSING_REQUIRED, "Field is mandatory but found empty.")

                    # --- C. DATETIME PROCESSING ---
                    if rules.get("type") == "datetime" and not is_null:
                        current_val = process_datetime(current_val, rules)

                    # --- D. SINGLE COLUMN UNIQUENESS ---
                    if rules.get("unique") and not is_null:
                        if col_name not in seen_singles: seen_singles[col_name] = set()
                        if current_val in seen_singles[col_name]:
                            raise DataValidationError(ErrorType.DUPLICATE_ENTRY, f"Duplicate value found: {current_val}")
                        seen_singles[col_name].add(current_val)

                    # --- E. COMPOSITE UNIQUENESS (The 'X' Bug Fix) ---
                    if "unique_composite" in rules:
                        # Create the key using the current column and its partners defined in YAML
                        composite_values = [str(current_val).strip().lower()]
                        for partner_col in rules["unique_composite"]:
                            p_val = str(row.get(partner_col, "")).strip().lower()
                            composite_values.append(p_val)
                        
                        comp_key = tuple(composite_values)
                        if comp_key in seen_composites:
                            raise DataValidationError(ErrorType.COMPOSITE_DUPLICATE, f"Composite key {comp_key} already exists.")
                        seen_composites.add(comp_key)

                    # --- F. PATTERN MATCHING (REGEX) ---
                    if "pattern" in rules and not is_null:
                        if not check_pattern(str(current_val), rules["pattern"]):
                            raise DataValidationError(ErrorType.PATTERN_MISMATCH, f"Value '{current_val}' does not match required pattern.")

                    # --- G. COMPLEX BUSINESS RULES (from rules.py) ---
                    # Note: We pass the full 'row' to allow cross-column validation
                    is_valid, msg = validate_business_rules(rules, current_val, row)
                    if not is_valid:
                        raise DataValidationError(ErrorType.BUSINESS_RULE_VIOLATION, msg)

                    # If everything is fine, store the processed value
                    temp_processed_row[col_name] = current_val

                except DataValidationError as e:
                    row_errors.append({
                        "Row_Number": global_row_counter,
                        "Column_Name": col_name,
                        "Invalid_Value": original_val,  # PRESERVED: Keeping the bad data for the report
                        "Error_Type": e.error_type,
                        "Severity": rules.get("priority", 2),
                        "Error_Message": e.message
                    })
                except Exception as e:
                    row_errors.append({
                        "Row_Number": global_row_counter,
                        "Column_Name": col_name,
                        "Invalid_Value": original_val,
                        "Error_Type": ErrorType.SYSTEM_CRASH,
                        "Severity": 1,
                        "Error_Message": f"Internal Error: {str(e)}"
                    })

            # --- FINAL VALIDATION DECISION ---
            # If the row has ANY errors, we discard the entire row from the clean output
            if not row_errors:
                valid_rows.append(temp_processed_row)
            else:
                # Add all collected errors for this row to the global error list
                errors.extend(row_errors)
            
            global_row_counter += 1

    # 2. SAVE AND REPORT
    result_dir = get_result_path(config)
    summary = save_results(valid_rows, errors, result_dir, config)

    print("\n" + "="*60)
    print(f"🚀 PIPELINE FINISHED")
    print(f"📊 Rows Processed: {global_row_counter - 2}")
    print(f"✅ Clean Records: {len(valid_rows)}")
    print(f"❌ Error Entries:  {len(errors)}")
    print(f"⏱️  Duration:       {datetime.now() - start_time}")
    print("="*60)
    print(summary)

if __name__ == "__main__":
    run_pipeline()