import pandas as pd
import os
from datetime import datetime

def save_results(valid_rows, errors, output_dir, config):
    """Generates Excel outputs and the Execution Summary Log."""
    os.makedirs(output_dir, exist_ok=True)
    
    # Create DataFrame from valid rows
    df_valid = pd.DataFrame(valid_rows)

    # FIX: Remove timezone info before saving to Excel
    for col in df_valid.select_dtypes(include=['datetimetz']):
        df_valid[col] = df_valid[col].dt.tz_localize(None)
    
    # Save Clean Data
    df_valid.to_excel(os.path.join(output_dir, "cleaned_data.xlsx"), index=False)
    
    # Save Errors
    if errors:
        error_df = pd.DataFrame(errors)
        # Also clean timezones in errors if any exist there
        for col in error_df.select_dtypes(include=['datetimetz']):
            error_df[col] = error_df[col].dt.tz_localize(None)
            
        error_df = error_df.sort_values(by=["Severity", "Row_Number"])
        error_df.to_excel(os.path.join(output_dir, "validation_errors.xlsx"), index=False)
    
    # Summary Report
    total_processed = len(valid_rows) + len(set(e['Row_Number'] for e in errors))
    health_score = (len(valid_rows) / total_processed) * 100 if total_processed > 0 else 0
    
    log_content = f"""
========================================
📊 DATA VALIDATION LOG
========================================
Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Source: {config['source']['file']}
Valid Rows: {len(valid_rows)}
Error Count: {len(errors)}
Health Score: {health_score:.2f}%
========================================
"""
    with open(os.path.join(output_dir, "execution_summary.txt"), "w", encoding="utf-8") as f:
        f.write(log_content)
    return log_content
