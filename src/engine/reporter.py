import pandas as pd
import os
import json

def save_results(valid_rows, errors, output_dir, config):
    """
    Saves processed data and structured error logs.
    Optimized for large-scale data reporting.
    """
    
    # 1. SAVE VALID DATA
    valid_df = pd.DataFrame(valid_rows)
    valid_file = os.path.join(output_dir, "cleaned_data.xlsx")
    
    if not valid_df.empty:
        valid_df.to_excel(valid_file, index=False)
    
    # 2. SAVE STRUCTURED ERROR LOG (JSON for System Integration)
    error_file_json = os.path.join(output_dir, "validation_errors.json")
    with open(error_file_json, 'w', encoding='utf-8') as f:
        json.dump(errors, f, indent=4, ensure_ascii=False)
        
    # 3. SAVE HUMAN-READABLE ERROR LOG (Excel)
    error_file_excel = os.path.join(output_dir, "error_report.xlsx")
    if errors:
        error_df = pd.DataFrame(errors)
        # Sort by Severity and Row Number for better readability
        error_df = error_df.sort_values(by=["Severity", "Row_Number"])
        error_df.to_excel(error_file_excel, index=False)

    # 4. GENERATE SUMMARY STATISTICS
    total_errors = len(errors)
    unique_error_types = set(e['Error_Type'] for e in errors) if errors else []
    
    summary = (
        f"--- Execution Summary ---\n"
        f"Clean Data Saved: {valid_file}\n"
        f"Error Logs Saved: {error_file_json}\n"
        f"Total Errors:     {total_errors}\n"
        f"Error Categories: {list(unique_error_types)}\n"
    )
    
    return summary
