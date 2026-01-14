import pandas as pd

df = pd.read_excel("data/sample.xlsx")
print("Columns:")
print(df.columns.tolist())

print("\nFirst rows:")
print(df.head())
