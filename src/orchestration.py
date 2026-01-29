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
    ADVANCED ORCHESTRATOR (Production Grade)
    - Zero-Tolerance for duplicates: If a duplicate is found, 
      BOTH the original and the duplicate are moved to the error report.
    - Preserves invalid values for auditing.
    - Standardizes date output (Date only, no timestamp).
    """
    try:
        config = load_config()
    except Exception as e:
        print(f"CRITICAL: Configuration error: {e}")
        return

    errors = []
    # Temporary storage to track valid rows by their composite keys
    # Format: { composite_key_tuple: (row_data_dict, row_number) }
    valid_rows_buffer = {}
    
    # Track which composite keys have already been flagged as duplicates
    blacklisted_composites = set()
    
    start_time = datetime.now()
    global_row_counter = 2 

    # 1. PROCESS DATA
    for chunk_df in load_source_chunks(config, chunk_size=chunk_size):
        for index, row in chunk_df.iterrows():
            row_errors = []
            temp_processed_row = {}
            current_row_composite_key = None

            for col_name, rules in config["columns"].items():
                original_val = row.get(col_name)
                
                try:
                    # --- A. CLEANING & NULL HANDLING ---
                    current_val = original_val
                    is_null = pd.isna(original_val) or str(original_val).strip() == ""
                    
                    if not is_null:
                        if rules.get("auto_clean"):
                            current_val = str(original_val).strip()
                        if rules.get("normalize"):
                            current_val = str(current_val).lower()

                    # --- B. MANDATORY CHECK ---
                    if rules.get("required") and is_null:
                        raise DataValidationError(ErrorType.MISSING_REQUIRED, "Mandatory field is empty")

                    # --- C. DATETIME (Date-Only Standardization) ---
                    if rules.get("type") == "datetime" and not is_null:
                        dt_obj = process_datetime(current_val, rules)
                        current_val = dt_obj.date() if hasattr(dt_obj, 'date') else dt_obj

                    # --- D. COMPOSITE UNIQUENESS GUARD (Double-Sided Logic) ---
                    if "unique_composite" in rules:
                        comp_parts = [str(current_val).strip().lower()]
                        for partner in rules["unique_composite"]:
                            comp_parts.append(str(row.get(partner, "")).strip().lower())
                        
                        current_row_composite_key = tuple(comp_parts)

                        if current_row_composite_key in blacklisted_composites:
                            raise DataValidationError(ErrorType.COMPOSITE_DUPLICATE, f"Composite duplicate found (Blacklisted): {current_row_composite_key}")

                        if current_row_composite_key in valid_rows_buffer:
                            # Conflict found: Retrieve and remove the first occurrence
                            prev_row_data, prev_row_num = valid_rows_buffer.pop(current_row_composite_key)
                            
                            # Move the first occurrence to errors
                            errors.append({
                                "Row_Number": prev_row_num,
                                "Column_Name": col_name,
                                "Invalid_Value": prev_row_data.get(col_name),
                                "Error_Type": ErrorType.COMPOSITE_DUPLICATE,
                                "Severity": rules.get("priority", 1),
                                "Error_Message": f"Original row flagged: Conflict with row {global_row_counter}"
                            })
                            
                            blacklisted_composites.add(current_row_composite_key)
                            raise DataValidationError(ErrorType.COMPOSITE_DUPLICATE, f"Duplicate of row {prev_row_num}")

                    # --- E. PATTERN & BUSINESS RULES ---
                    if "pattern" in rules and not is_null:
                        if not check_pattern(str(current_val), rules["pattern"]):
                            raise DataValidationError(ErrorType.PATTERN_MISMATCH, f"Invalid pattern: {current_val}")

                    is_valid, msg = validate_business_rules(rules, current_val, row)
                    if not is_valid:
                        raise DataValidationError(ErrorType.BUSINESS_RULE_VIOLATION, msg)

                    temp_processed_row[col_name] = current_val

                except DataValidationError as e:
                    row_errors.append({
                        "Row_Number": global_row_counter,
                        "Column_Name": col_name,
                        "Invalid_Value": original_val,
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
                        "Error_Message": f"System Error: {str(e)}"
                    })

            # --- FINAL COMMIT DECISION ---
            if not row_errors:
                if current_row_composite_key:
                    valid_rows_buffer[current_row_composite_key] = (temp_processed_row, global_row_counter)
                else:
                    valid_rows_buffer[f"single_{global_row_counter}"] = (temp_processed_row, global_row_counter)
            else:
                errors.extend(row_errors)
            
            global_row_counter += 1

    # Convert buffer back to a flat list for saving
    valid_rows = [item[0] for item in valid_rows_buffer.values()]

    # 2. EXPORT & FINAL SUMMARY
    result_dir = get_result_path(config)
    summary = save_results(valid_rows, errors, result_dir, config)

    # --- PROFESSIONAL CONSOLE OUTPUT ---
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