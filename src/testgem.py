import pandas as pd
import yaml

# ۱. بارگذاری تنظیمات
with open("config/mapping.yaml", "r") as f:
config = yaml.safe_load(f)

# ۲. خواندن فایل اکسل اصلی
df = pd.read_excel(
config["source"]["file"],
sheet_name=config["source"]["sheet"]
)

errors = []
valid_rows = []

# ۳. پردازش سطر به سطر
for index, row in df.iterrows():
row_errors = []
processed_row = {}

for col_name, rules in config["columns"].items():
value = row.get(col_name)
target_name = rules["target"]
expected_type = rules["type"]
is_required = rules["required"]

# الف) بررسی خالی بودن (Required)
if is_required and pd.isna(value):
row_errors.append(f"ستون '{col_name}' اجباری است اما مقدار ندارد.")
continue

# ب) بررسی نوع داده (Type Validation)
if not pd.isna(value):
try:
if expected_type == "int":
value = int(value)
elif expected_type == "float":
value = float(value)
elif expected_type == "string":
value = str(value)
except (ValueError, TypeError):
row_errors.append(f"نوع داده '{col_name}' باید {expected_type} باشد.")
continue

processed_row[target_name] = value

# ۴. دسته‌بندی سطرها بر اساس خطا
if row_errors:
errors.append({
"Line_Number": index + 2, # شماره سطر در اکسل
"Errors": " | ".join(row_errors),
**row.to_dict() # کل سطر اصلی را برای بررسی نگه می‌داریم
})
else:
valid_rows.append(processed_row)

# ۵. ایجاد دیتافریم‌های خروجی
output_df = pd.DataFrame(valid_rows)
error_df = pd.DataFrame(errors)

# ۶. ذخیره در فایل اکسل
if not error_df.empty:
error_df.to_excel("validation_errors.xlsx", index=False)
print(f"تعداد {len(error_df)} خطا پیدا شد. جزئیات در فایل validation_errors.xlsx ذخیره شد.")

if not output_df.empty:
output_df.to_excel("cleaned_data.xlsx", index=False)
print("داده‌های سالم در فایل cleaned_data.xlsx ذخیره شدند.")
