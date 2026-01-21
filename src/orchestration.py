import sys
import os
from engine.loader import load_config, load_data
# ... بقیه ایمپورت‌ها

def run_pipeline():
    """Main function to run the validation orchestration."""
    # کل منطق اجرای برنامه (حلقه For و لود کردن دیتایی که قبلاً نوشتیم) را اینجا بگذارید
    config = load_config("../mappings/mapping.yaml")
    df = load_data(config)
    # ... بقیه کد ...
    print("Validation finished.")

if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as e:
        print(f"❌ Critical Error during orchestration: {e}")
