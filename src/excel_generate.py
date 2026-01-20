import pandas as pd
import random
from datetime import datetime, timedelta

def generate_presentation_data():
    num_rows = 200
    data = []
    now = datetime.now()

    for i in range(num_rows):
        # 1. customr_id
        c_id = 1000 + i if i != 10 else 1005 # Intentional Duplicate for row 10
        
        # 2. name
        name = f"Company_{i}"
        if i == 20: name = "  CleanMe  " # Test Auto-clean
        
        # 3. status
        status = random.choice(["Active", "Deactive", "Suspend"])
        
        # 4. amount (Test DSL Conditional: If Suspend, amount must be 0)
        if status == "Suspend":
            amount = 0 if i % 5 != 0 else 500 # Row with error if i%5==0
        else:
            amount = round(random.uniform(1000, 5000), 2)
            
        # 5. project_code (Test Regex)
        p_code = f"PRJ-{random.randint(100, 999)}" if i % 15 != 0 else "INVALID-123"
        
        # 6. created_at (Test Future Date and Serial Date)
        if i == 30:
            created_at = (now + timedelta(days=5)) # Future date Error
        elif i == 40:
            created_at = 45321.5 # Excel Serial Date for May 2024
        else:
            created_at = now - timedelta(days=i)

        data.append([c_id, name, amount, p_code, status, created_at])

    df = pd.DataFrame(data, columns=['customr_id', 'name', 'amount', 'project_code', 'status', 'created_at'])
    df.to_excel("data/sample.xlsx", index=False)
    print("✅ New test data generated successfully!")

if __name__ == "__main__":
    generate_presentation_data()
