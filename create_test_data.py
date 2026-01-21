import pandas as pd
import os

def generate_test_excel():
    rows = []
    
    # 1. VALID DATA (20 Rows)
    for i in range(1, 21):
        rows.append({
            "name": f"Valid User {i}",
            "status": "active",
            "amount": 1000 + i,
            "created_at": "2026-01-10 10:00:00"
        })

    # 2. UNIQUENESS TEST (Should trigger Duplicate Error)
    rows.append({"name": "Valid User 1", "status": "active", "amount": 500, "created_at": "2026-01-11"})
    
    # 3. TYPE VALIDATION TEST (Should trigger TYPE_OR_VALIDATION_ERROR)
    rows.append({"name": "Type Error User", "status": "active", "amount": "INVALID_STRING", "created_at": "2026-01-12"})
    rows.append({"name": "Bad Date User", "status": "active", "amount": 200, "created_at": "Not-A-Date"})

    # 4. LENGTH RULE TEST (Should trigger Business Rule Violation)
    rows.append({"name": "A", "status": "active", "amount": 300, "created_at": "2026-01-13"}) # Too short
    rows.append({"name": "X" * 60, "status": "active", "amount": 400, "created_at": "2026-01-14"}) # Too long

    # 5. ALLOWED VALUES TEST (Should trigger Business Rule Violation)
    rows.append({"name": "Wrong Status User", "status": "unknown_status", "amount": 100, "created_at": "2026-01-15"})

    # 6. CONDITIONAL RULE TEST (Suspend logic: Amount must be 0)
    rows.append({"name": "Suspend Fail", "status": "suspend", "amount": 9999, "created_at": "2026-01-16"}) 
    rows.append({"name": "Suspend Pass", "status": "suspend", "amount": 0, "created_at": "2026-01-17"}) 

    # 7. DATE RANGE & EXCEL SERIAL TEST
    rows.append({"name": "Future Date User", "status": "active", "amount": 100, "created_at": "2027-01-01"})
    rows.append({"name": "Extreme Excel Date", "status": "active", "amount": 100, "created_at": 4654654.23})

    # 8. FILL REMAINING (Up to 200 rows with random-like valid data)
    for i in range(len(rows) + 1, 201):
        rows.append({
            "name": f"Random User {i}",
            "status": "pending",
            "amount": i * 5,
            "created_at": "2025-12-25"
        })

    # Create directory if not exists
    os.makedirs("data", exist_ok=True)
    
    df = pd.DataFrame(rows)
    output_path = "data/input_data.xlsx"
    df.to_excel(output_path, index=False)
    print(f"SUCCESS: Test file generated at {output_path}")

if __name__ == "__main__":
    generate_test_excel()
