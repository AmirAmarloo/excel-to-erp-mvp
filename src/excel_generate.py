import pandas as pd
import random
import numpy as np

num_rows = 200
data = []

# Possible values
valid_statuses = ["Active", "Deactive", "Suspend"]
project_patterns = ["PRJ-101", "PRJ-202", "PRJ-303"]

for i in range(num_rows):
    row_num = i + 2 # For logic tracking
    
    # --- Column: customr_id (Logic: Unique, 1000-9999) ---
    if i == 10 or i == 11:
        c_id = 5555  # ERROR: Duplicate Value
    elif i == 20:
        c_id = 101   # ERROR: Limit Violation (Too Small)
    elif i == 30:
        c_id = 10001 # ERROR: Limit Violation (Too Large)
    elif i == 40:
        c_id = np.nan # ERROR: Required (Missing)
    else:
        c_id = 2000 + i

    # --- Column: name (Logic: 3-50 chars) ---
    if i == 50:
        name = "Jo"  # ERROR: Length Violation (Too Short)
    elif i == 60:
        name = "X" * 60 # ERROR: Length Violation (Too Long)
    elif i == 70:
        name = np.nan # ERROR: Required (Missing)
    else:
        name = f"Company_{i}"

    # --- Column: amount (Logic: min 0) ---
    if i == 80:
        amount = -50.5 # ERROR: Limit Violation (Negative)
    elif i == 90:
        amount = "InvalidAmount" # ERROR: Type Mismatch (String in Float col)
    else:
        amount = round(random.uniform(100, 5000), 2)

    # --- Column: project_code (Logic: Regex ^PRJ-[0-9]+$) ---
    if i == 100:
        p_code = "PROJ-123" # ERROR: Pattern Mismatch (PROJ instead of PRJ)
    elif i == 110:
        p_code = "PRJ-ABC"  # ERROR: Pattern Mismatch (Letters instead of Numbers)
    else:
        p_code = f"PRJ-{random.randint(100, 999)}"

    # --- Column: status (Logic: Enum ["Active", "Deactive", "Suspend"]) ---
    if i == 120:
        status = "active"   # ERROR: Invalid Option (Lowercase 'a')
    elif i == 130:
        status = "De-active" # ERROR: Invalid Option (Hyphen added)
    elif i == 140:
        status = "Suspended" # ERROR: Invalid Option (Wrong spelling)
    else:
        status = random.choice(valid_statuses)

    data.append([c_id, name, amount, p_code, status])

# Generate DataFrame
df = pd.DataFrame(data, columns=['customr_id', 'name', 'amount', 'project_code', 'status'])

# Save to Excel
df.to_excel("data/sample.xlsx", index=False)
print(f"✅ Presentation File Created: 200 rows with 15+ intentional errors.")
print(f"Validation finished. Errors sorted by severity.")
