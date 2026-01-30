import pandas as pd
import random
import os

def generate_dirty_demo():
    rows = 75000
    data = []
    print(f"Generating {rows} rows of dirty data for stress test...")

    for i in range(rows):
        # Default Valid
        proj, cust, inv = f"PRJ-{i}", f"CUST-{i}", f"INV-{i}"
        email = f"user{i}@example.com"
        name, status, amount, date = f"Company {i}", "active", 100.0, "2024-01-01"

        # 1. Missing Required
        if i == 100: cust = None
        # 2. Pattern Mismatch
        if i == 200: proj = "WRONG-CODE"
        # 3. Business Rule (Suspend must be 0)
        if i == 300: status, amount = "suspend", 5000
        # 4. Composite Duplicate (3 Cases)
        if i in [400, 401]: inv, cust = "DUP-01", "CUST-X"
        if i in [500, 501, 502]: inv, cust = "DUP-02", "CUST-Y"
        # 5. Date Future
        if i == 600: date = "2035-01-01"
        # 6. Email format
        if i == 700: email = "invalid_email.com"
        # 7. Data Type Error
        if i == 800: amount = "TEXT_DATA"

        data.append({
            "project_code": proj, "customer_code": cust, "invoice_number": inv,
            "email": email, "name": name, "status": status, 
            "amount": amount, "created_at": date
        })

    df = pd.DataFrame(data)
    output_path = "data/input_data.csv" # Saved as CSV for speed
    os.makedirs("data", exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"✅ Stress-test CSV created: {output_path}")

if __name__ == "__main__":
    generate_dirty_demo()
