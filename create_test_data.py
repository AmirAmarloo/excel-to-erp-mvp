import pandas as pd
import os

def generate_test_excel():
    rows = []
    
    # 1. VALID DATA (Rows with Customer Code)
    for i in range(1, 21):
        rows.append({
            "customer_code": f"CUST-{100+i}",
            "name": f"Valid User {i}",
            "status": "active",
            "amount": 1000 + i,
            "created_at": "2026-01-10"
        })

    # 2. TEST: DUPLICATE CUSTOMER CODE
    rows.append({"customer_code": "CUST-101", "name": "Duplicate Code User", "status": "active", "amount": 500, "created_at": "2026-01-11"})
    
    # 3. TEST: INVALID CODE PATTERN
    rows.append({"customer_code": "WRONG_99", "name": "Pattern Fail User", "status": "active", "amount": 200, "created_at": "2026-01-12"})

    # 4. TEST: SUSPEND RULE (Amount must be 0)
    rows.append({"customer_code": "CUST-500", "name": "Suspend Fail", "status": "suspend", "amount": 5000, "created_at": "2026-01-13"})

    # 5. TEST: EXTREME EXCEL DATE (4654654.23)
    rows.append({"customer_code": "CUST-600", "name": "Crazy Date User", "status": "active", "amount": 100, "created_at": 4654654.23})

    # 6. FILL REMAINING UP TO 200 ROWS
    for i in range(len(rows) + 1, 201):
        rows.append({
            "customer_code": f"CUST-{1000+i}",
            "name": f"Random User {i}",
            "status": "pending",
            "amount": i * 2,
            "created_at": "2025-12-01"
        })

    os.makedirs("data", exist_ok=True)
    df = pd.DataFrame(rows)
    output_path = "data/input_data.xlsx"
    df.to_excel(output_path, index=False)
    print(f"SUCCESS: Test file generated with Customer Codes at {output_path}")

if __name__ == "__main__":
    generate_test_excel()
