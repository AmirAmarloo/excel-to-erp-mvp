import pandas as pd
import re
from datetime import datetime

def process_datetime(value, rules):
    """
    Processes datetime values with strict validation for Excel serial numbers.
    """
    try:
        # Check if the value is a number (Excel serial date)
        if isinstance(value, (int, float)):
            # Heuristic: Excel dates for the 21st century (2000-2100) 
            # fall between 36526 and 73050. 
            # 4321.5 is way too small (it's in the year 1911).
            if value < 30000: # 30000 is approx year 1982
                raise ValueError(f"Value '{value}' is too small to be a valid modern date.")
            
            if rules.get("excel_date"):
                dt_obj = pd.to_datetime(value, unit='D', origin='1899-12-30')
            else:
                # If excel_date is not allowed in config, reject numeric input
                raise ValueError("Numeric dates are not allowed for this field.")
        else:
            # If it's a string, parse it normally
            dt_obj = pd.to_datetime(value, dayfirst=True)

        # Timezone localization
        target_tz = rules.get("timezone", "UTC")
        if dt_obj.tzinfo is None:
            dt_obj = dt_obj.tz_localize('UTC').tz_convert(target_tz)
        else:
            dt_obj = dt_obj.tz_convert(target_tz)
            
        return dt_obj

    except Exception as e:
        # This will be caught by orchestration.py and added to the error list
        raise ValueError(f"Invalid date format: {value}. {str(e)}")

def check_pattern(value, pattern):
    """
    Validates a string against a regex pattern.
    """
    return bool(re.fullmatch(str(pattern), str(value)))
