import pandas as pd
import random
import os

def generate_demo_data():
    rows = 200 # For demo, 200 rows is enough to see every error type clearly
    data = []
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_path = os.path.join(base_dir, "data", "input_data.xlsx")

    for i in range(rows):
        # Default Valid Data
        proj = f"PRJ-{1000 + i}"
        cust = f"CUST-{2000 + i}"
        inv = f"INV-{3000 + i}"
        name = f"German Tech GmbH {i}"
        status = "active"
        amount = 500.0
        date = "2024-01-01"

        # --- STRATEGIC ERROR INJECTION ---
        if i == 10: proj = "INVALID-123"           # Pattern Mismatch
        if i == 20: cust = None                    # Required Missing
        if i == 30: inv = "X"                      # Composite Duplicate Part A
        if i == 31: inv, cust = "X", "CUST-2030"   # Composite Duplicate Part B
        if i == 40: name = "Ab"                    # Min Length Violation
        if i == 50: status = "draft"               # Allowed Values Violation
        if i == 60: status, amount = "suspend", 10 # Conditional Violation (must be 0)
        if i == 70: date = "2029-05-05"            # Future Date Violation
        if i == 80: amount = "NOT_A_NUMBER"        # Type Mismatch (Float)

        data.append({
            "project_code": proj, "customer_code": cust, "invoice_number": inv,
            "name": name, "status": status, "amount": amount, "created_at": date
        })

    df = pd.DataFrame(data)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_excel(output_path, index=False)
    print(f"✅ Full-Scenario Demo Data created at: {output_path}")

if __name__ == "__main__":
    generate_demo_data()