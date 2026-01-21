import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_test_excel():
    rows = []
    
    # 1. ردیف‌های کاملاً سالم (20 ردیف)
    for i in range(1, 21):
        rows.append({
            "name": f"Valid User {i}",
            "status": "active",
            "amount": 1000 + i,
            "created_at": "2026-01-10 10:00:00"
        })

    # 2. تست تکراری بودن (Duplicate Name)
    rows.append({"name": "Valid User 1", "status": "active", "amount": 500, "created_at": "2026-01-11"})
    
    # 3. تست تایپ اشتباه (Type Error)
    rows.append({"name": "Type Error User", "status": "active", "amount": "NOT_A_NUMBER", "created_at": "2026-01-12"})
    rows.append({"name": "Bad Date User", "status": "active", "amount": 200, "created_at": "Invalid-Date-String"})

    # 4. تست طول نام (Length Rule)
    rows.append({"name": "A", "status": "active", "amount": 300, "created_at": "2026-01-13"}) # Too short
    rows.append({"name": "Long Name " * 10, "status": "active", "amount": 400, "created_at": "2026-01-14"}) # Too long

    # 5. تست وضعیت نامعتبر (Allowed Values)
    rows.append({"name": "Invalid Status User", "status": "sd", "amount": 100, "created_at": "2026-01-15"})

    # 6. تست شرط تعلیق (Suspend Condition)
    rows.append({"name": "Suspend Fail", "status": "suspend", "amount": 5000, "created_at": "2026-01-16"}) # Amount must be 0
    rows.append({"name": "Suspend Success", "status": "suspend", "amount": 0, "created_at": "2026-01-17"}) # Valid

    # 7. تست تاریخ کذایی (Excel Serial & Future)
    rows.append({"name": "Future Date", "status": "active", "amount": 100, "created_at": "2027-01-01"})
    rows.append({"name": "Crazy Excel Number", "status": "active", "amount": 100, "created_at": 4654654.23})

    # 8. پر کردن باقی‌مانده تا 200 ردیف با داده‌های تصادفی
    for i in range(len(rows) + 1, 201):
        rows.append({
            "name": f"Random User {i}",
            "status": "pending",
            "amount": i * 10,
            "created_at": "2025-12-01"
        })

    df = pd.DataFrame(rows)
    output_path = "data/input_data.xlsx"
    df.to_excel(output_path, index=False)
    print(f"✅ فایل تست با موفقیت در مسیر {output_path} ساخته شد.")

if __name__ == "__main__":
    generate_test_excel()
