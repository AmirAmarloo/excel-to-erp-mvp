import pandas as pd
import os
from datetime import datetime

def save_results(valid_rows, errors, output_dir, config):
    """Generates Excel outputs and the Execution Summary Log."""
    os.makedirs(output_dir, exist_ok=True)
    
    # Save Clean Data
    pd.DataFrame(valid_rows).to_excel(os.path.join(output_dir, "cleaned_data.xlsx"), index=False)
    
    # Save Errors
    if errors:
        error_df = pd.DataFrame(errors).sort_values(by=["Severity", "Row_Number"])
        error_df.to_excel(os.path.join(output_dir, "validation_errors.xlsx"), index=False)
    
    # Summary Report
    health_score = (len(valid_rows) / (len(valid_rows) + len(set(e['Row_Number'] for e in errors)))) * 100 if (valid_rows or errors) else 0
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
