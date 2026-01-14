#import pandas as pd

#df = pd.read_excel("data/sample.xlsx")
#print("Columns:")
#print(df.columns.tolist())

#print("\nFirst rows:")
#print(df.head())


import pandas as pd
import yaml

# Load config
with open("mappings/mapping.yaml", "r") as f:
    config = yaml.safe_load(f)

# Load Excel
df = pd.read_excel(
    config["source"]["file"],
    sheet_name=config["source"]["sheet"]
)

errors = []
output_rows = []

for index, row in df.iterrows():
    out = {}
    for col, rules in config["columns"].items():
        value = row.get(col)

        if rules["required"] and pd.isna(value):
            errors.append({
                "row": index + 2,
                "column": col,
                "error": "Required value missing"
            })
            break

        out[rules["target"]] = value

    else:
        output_rows.append(out)

output_df = pd.DataFrame(output_rows)
error_df = pd.DataFrame(errors)

print("Valid rows:")
print(output_df)

print("\nErrors:")
print(error_df)


