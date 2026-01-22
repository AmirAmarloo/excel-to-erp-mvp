import pandas as pd
import os

def generate_test_excel():
    rows = []
    # Valid rows with same customer but different invoices
    for i in range(1, 11):
        rows.append({
            "project_code": f"PRJ-{200+i}",
            "customer_code": "CUST-100",
            "invoice_number": f"INV-{i}",
            "name": "Global Corp",
            "status": "ACTIVE", # Testing normalize: true (to lower)
            "amount": 500.0,
            "created_at": "2025-05-10"
        })
    
    # Fail: Composite Duplicate (Same Customer + Same Invoice)
    rows.append({"project_code": "PRJ-500", "customer_code": "CUST-100", "invoice_number": "INV-1", "name": "Fail", "status": "active", "amount": 10, "created_at": "2025-01-01"})
    
    # Fail: Suspend Logic
    rows.append({"project_code": "PRJ-600", "customer_code": "CUST-200", "invoice_number": "INV-99", "name": "Suspend Fail", "status": "suspend", "amount": 100, "created_at": "2025-01-01"})

    os.makedirs("data", exist_ok=True)
    pd.DataFrame(rows).to_excel("data/input_data.xlsx", index=False)
    print("Test file created.")

if __name__ == "__main__":
    generate_test_excel()
