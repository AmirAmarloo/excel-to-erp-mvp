import pandas as pd
import random
from datetime import datetime, timedelta
import numpy as np

num_rows = 200
data = []

valid_statuses = ["Active", "Deactive", "Suspend"]
project_patterns = ["PRJ-101", "PRJ-202", "PRJ-303"]

# Current time for date calculations
now = datetime.now()

for i in range(num_rows):
    # 1. customr_id (Logic: Unique, 1000-9999)
    if i == 5 or i == 6:
        c_id = 7777 # ERROR: Duplicate
    elif i == 15:
        c_id = 101  # ERROR: Below min_value
    elif i == 25:
        c_id = " 5000 " # TEST: auto_clean (Should pass after trimming)
    else:
        c_id = 2000 + i

    # 2. name (Logic: 3-50 chars)
    if i == 35:
        name = "AB" # ERROR: Too short
    elif i == 45:
        name = "   Customer_Clean   " # TEST: auto_clean
    else:
        name = f"Client_Node_{i}"

    # 3. status (Logic: Allowed values)
    if i == 55:
        status = "Active " # TEST: auto_clean (Should fix the space)
    elif i == 65:
        status = "Pending" # ERROR: Invalid Option
    else:
        status = random.choice(valid_statuses)

    # 4. amount (Logic: Custom rule - If Suspend, must be 0)
    if status == "Suspend" and i % 2 == 0:
        amount = 500.0 # ERROR: Custom logic violation (Suspend but amount > 0)
    elif i == 75:
        amount = -10 # ERROR: min_value violation
    else:
        amount = 0.0 if status == "Suspend" else round(random.uniform(500, 2500), 2)

    # 5. created_at (Logic: Datetime, not in future)
    if i == 85:
        # ERROR: Future date (Custom logic violation)
        created_at = (now + timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S")
    elif i % 10 == 0:
        # Valid: Past date with different format
        created_at = (now - timedelta(days=i)).strftime("%d/%m/%Y %H:%M")
    else:
        created_at = now.strftime("%Y-%m-%d %H:%M:%S")

    # 6. project_code
    p_code = random.choice(project_patterns) if i % 12 != 0 else "INVALID-CODE"

    data.append([c_id, name, amount, p_code, status, created_at])

# Create DataFrame and Save
df = pd.DataFrame(data, columns=['customr_id', 'name', 'amount', 'project_code', 'status', 'created_at'])
df.to_excel("data/sample.xlsx", index=False)

print(f"🚀 Success! Presentation file 'data/sample.xlsx' generated.")
print(f"📊 Includes: Duplicates, Future Dates, Cleaning Tests, and Logic Conflicts.")
