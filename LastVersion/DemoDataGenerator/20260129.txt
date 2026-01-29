import pandas as pd
import os
import random
from datetime import datetime

def generate_test_excel():
    rows = []
    
    # 1. VALID DATA (30 Rows) - Base for success
    for i in range(1, 31):
        rows.append({
            "project_code": f"PRJ-{100+i}",
            "customer_code": f"CUST-{200+i}",
            "invoice_number": f"INV-{1000+i}",
            "name": f"Valid Customer {i}",
            "status": "active",
            "amount": 1500.0,
            "created_at": "2025-06-15"
        })

    # 2. TYPE & MANDATORY TESTS (Invalid Types / Missing Values)
    rows.append({"project_code": "PRJ-999", "customer_code": "CUST-999", "invoice_number": "INV-2000", "name": "Missing Amount", "status": "active", "amount": None, "created_at": "2025-01-01"})
    rows.append({"project_code": "PRJ-999", "customer_code": "CUST-999", "invoice_number": "INV-2001", "name": "Wrong Amount Type", "status": "active", "amount": "TEN_DOLLARS", "created_at": "2025-01-01"})

    # 3. LENGTH & PATTERN TESTS (Project Code & Name)
    rows.append({"project_code": "WRONG-FORMAT", "customer_code": "CUST-888", "invoice_number": "INV-3000", "name": "Bad Project Pattern", "status": "active", "amount": 100, "created_at": "2025-01-01"})
    rows.append({"project_code": "PRJ-888", "customer_code": "CUST-888", "invoice_number": "INV-3001", "name": "Ab", "status": "active", "amount": 100, "created_at": "2025-01-01"}) # Name too short

    # 4. CUSTOM RULE: SUSPEND LOGIC (Action: Must be 0)
    rows.append({"project_code": "PRJ-777", "customer_code": "CUST-777", "invoice_number": "INV-4000", "name": "Suspend With Amount Fail", "status": "suspend", "amount": 500.5, "created_at": "2025-01-01"})
    rows.append({"project_code": "PRJ-777", "customer_code": "CUST-777", "invoice_number": "INV-4001", "name": "Suspend With Zero Success", "status": "suspend", "amount": 0, "created_at": "2025-01-01"})

    # 5. DATE CONTROL (Future Date & Old Date & Excel Serial)
    rows.append({"project_code": "PRJ-666", "customer_code": "CUST-666", "invoice_number": "INV-5000", "name": "Future Date Fail", "status": "active", "amount": 100, "created_at": "2027-01-01"})
    rows.append({"project_code": "PRJ-666", "customer_code": "CUST-667", "invoice_number": "INV-5001", "name": "Excel Serial Date Fail", "status": "active", "amount": 100, "created_at": 4654654.23})

    # 6. UNIQUENESS & COMPOSITE KEY (Invoice Number per Customer)
    # Valid: Same customer, different invoice
    rows.append({"project_code": "PRJ-555", "customer_code": "CUST-555", "invoice_number": "INV-UNIQUE-1", "name": "Unique Invoice", "status": "active", "amount": 100, "created_at": "2025-01-01"})
    # Fail: Same customer, same invoice
    rows.append({"project_code": "PRJ-555", "customer_code": "CUST-555", "invoice_number": "INV-UNIQUE-1", "name": "Duplicate Invoice Fail", "status": "active", "amount": 100, "created_at": "2025-01-01"})

    # 7. FILLING REMAINING TO REACH 200 ROWS
    current_count = len(rows)
    for i in range(current_count + 1, 201):
        status_choice = random.choice(["active", "pending", "closed"])
        rows.append({
            "project_code": f"PRJ-{2000+i}",
            "customer_code": f"CUST-{3000+i}",
            "invoice_number": f"INV-{5000+i}",
            "name": f"Automated User {i}",
            "status": status_choice,
            "amount": float(random.randint(100, 10000)),
            "created_at": "2025-10-10"
        })

    # Saving the file
    os.makedirs("data", exist_ok=True)
    df = pd.DataFrame(rows)
    output_path = "data/input_data.xlsx"
    df.to_excel(output_path, index=False)
    
    print(f"✅ SUCCESS: {len(df)} rows generated.")
    print(f"📍 File Location: {output_path}")
    print("⚠️  This file contains deliberate errors for Testing all your Validation Rules.")

if __name__ == "__main__":
    generate_test_excel()
