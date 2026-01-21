import pandas as pd
import re

def process_datetime(value, rules):
    """Processes Excel dates and timezones."""
    if rules.get("excel_date") and isinstance(value, (int, float)):
        # Convert Excel serial number to datetime
        dt_obj = pd.to_datetime(value, unit='D', origin='1899-12-30')
    else:
        # Convert string to datetime
        dt_obj = pd.to_datetime(value, dayfirst=True)
    
    target_tz = rules.get("timezone", "UTC")
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.tz_localize('UTC').tz_convert(target_tz)
    else:
        dt_obj = dt_obj.tz_convert(target_tz)
    return dt_obj

def check_pattern(value, pattern):
    """Checks if string matches regex pattern."""
    return bool(re.fullmatch(str(pattern), str(value)))
