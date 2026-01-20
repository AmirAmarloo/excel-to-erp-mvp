import pandas as pd
import re

def process_datetime(value, rules):
    """Handles Excel serial dates and Timezone localization."""
    if rules.get("excel_date") and isinstance(value, (int, float)):
        dt_obj = pd.to_datetime(value, unit='D', origin='1899-12-30')
    else:
        dt_obj = pd.to_datetime(value, dayfirst=True)
    
    target_tz = rules.get("timezone", "UTC")
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.tz_localize('UTC').tz_convert(target_tz)
    else:
        dt_obj = dt_obj.tz_convert(target_tz)
    return dt_obj

def check_pattern(value, pattern):
    """Validates string against Regex pattern."""
    return bool(re.fullmatch(str(pattern), str(value)))
