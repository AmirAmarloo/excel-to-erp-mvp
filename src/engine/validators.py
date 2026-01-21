import pandas as pd
import re
from datetime import datetime

def process_datetime(value, rules):
    """
    Processes datetime strings and Excel serial numbers.
    Ensures the output is a timezone-aware pandas Timestamp.
    """
    try:
        # 1. Handle Numeric input (Excel Serial Dates)
        if isinstance(value, (int, float)):
            # Validate if it's a realistic modern Excel date (e.g., > year 1990)
            if value < 32874: # 32874 is approx 1990-01-01
                raise ValueError(f"Numeric value {value} is too old or invalid.")
            
            if rules.get("excel_date"):
                dt_obj = pd.to_datetime(value, unit='D', origin='1899-12-30')
            else:
                raise ValueError("Numeric Excel dates are disabled in config.")
        
        # 2. Handle String input
        else:
            # pd.to_datetime is powerful enough to handle '4:18:26 PM' automatically
            dt_obj = pd.to_datetime(value, dayfirst=True)

        # 3. Timezone conversion (Default to UTC if not specified)
        target_tz = rules.get("timezone", "UTC")
        if dt_obj.tzinfo is None:
            dt_obj = dt_obj.tz_localize('UTC').tz_convert(target_tz)
        else:
            dt_obj = dt_obj.tz_convert(target_tz)
            
        return dt_obj

    except Exception as e:
        # This message will appear in your validation_errors.xlsx
        raise ValueError(f"Datetime processing failed: {str(e)}")

def check_pattern(value, pattern):
    """
    Validates a string value against a regex pattern.
    """
    return bool(re.fullmatch(str(pattern), str(value)))
