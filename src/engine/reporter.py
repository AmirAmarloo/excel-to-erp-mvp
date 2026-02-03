import pandas as pd
import os
import json

def save_results(valid_rows, errors, result_dir, config, execution_stats):
    """
    Saves clean data to Excel and error logs to both Excel and JSON.
    Also writes the exact console execution summary to a text file.
    """
    # 1. Ensure the directory exists (Automatic Infrastructure)
    if not os.path.exists(result_dir):
        os.makedirs(result_dir, exist_ok=True)
        print(f"📁 Created missing directory: {result_dir}")

    # 2. Generate timestamps for unique filenames
    #timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    
    #valid_file = os.path.join(result_dir, f"clean_data_{timestamp}.xlsx")
    #error_file = os.path.join(result_dir, f"validation_errors_{timestamp}.xlsx")
    #json_error_file = os.path.join(result_dir, f"validation_errors_{timestamp}.json")
    #summary_file = os.path.join(result_dir, f"summary_execution_{timestamp}.txt")

    valid_file = os.path.join(result_dir, f"clean_data.xlsx")
    error_file = os.path.join(result_dir, f"validation_errors.xlsx")
    json_error_file = os.path.join(result_dir, f"validation_errors.json")
    summary_file = os.path.join(result_dir, f"summary_execution.txt")

    # 3. Save Valid Records to Excel
    valid_df = pd.DataFrame(valid_rows)
    if not valid_df.empty:
        valid_df.to_excel(valid_file, index=False)
    
    # 4. Save Error Records (Excel & JSON)
    error_df = pd.DataFrame(errors)
    if not error_df.empty:
        error_df.to_excel(error_file, index=False)
        
        # Save JSON for ERP/System integration
        with open(json_error_file, "w", encoding="utf-8") as f:
            json.dump(errors, f, indent=4, default=str)

    # 5. Save the EXACT Console Summary to a text file (As requested)
    with open(summary_file, "w", encoding="utf-8") as f:
        f.write(execution_stats)

    return f"✨ Reports successfully saved to: {result_dir}"