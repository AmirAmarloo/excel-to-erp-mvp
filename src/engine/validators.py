import re
from datetime import datetime
import pandas as pd
from enum import Enum

class ErrorType:
    """Standardized Error Types for Machine-Readable Reporting"""
    TYPE_MISMATCH = "TYPE_MISMATCH"
    MISSING_REQUIRED = "MISSING_REQUIRED"
    DUPLICATE_ENTRY = "DUPLICATE_ENTRY"
    COMPOSITE_DUPLICATE = "COMPOSITE_DUPLICATE"
    PATTERN_MISMATCH = "PATTERN_MISMATCH"
    BUSINESS_RULE_VIOLATION = "BUSINESS_RULE_VIOLATION"
    INVALID_DATE = "INVALID_DATE"
    SYSTEM_CRASH = "SYSTEM_CRASH"

class DataValidationError(Exception):
    """
    Custom Exception for Structured Data Errors.
    Allows passing error_type for ERP/System integration.
    """
    def __init__(self, error_type, message, severity="High"):
        self.error_type = error_type
        self.message = message
        self.severity = severity
        super().__init__(self.message)
        
# Standard Email Regex
EMAIL_REGEX = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

def process_datetime(val, rules):
    """
    Validates and processes datetime objects.
    Preserves all previous logic for Excel serial dates and string formats.
    """
    if pd.isna(val) or val == "":
        return None

    #try:
        # If it's already a datetime object (from Excel)
     #   if isinstance(val, datetime):
      #      dt_obj = val
        # If it's a string, try to parse it
      #  elif isinstance(val, str):
            # Supports standard format: YYYY-MM-DD
      #      dt_obj = datetime.strptime(val.strip(), "%Y-%m-%d")
      #  else:
            # For any other types like Excel serial numbers
      #      dt_obj = pd.to_datetime(val)
        
       # return dt_obj
    #except Exception:
     #   raise DataValidationError(
      #      ErrorType.INVALID_DATE, 
       #     f"Value '{val}' is not a valid date (Expected YYYY-MM-DD)."
       # )

def process_datetime(val, rules):
    """
    Validates and processes datetime objects with smart parsing.
    Supports YYYY-MM-DD, DD/MM/YYYY, and German DD.MM.YYYY formats.
    """
    if pd.isna(val) or str(val).strip() == "":
        return None

    try:
        # 1. اگر از قبل شیء datetime است (مستقیم از اکسل)
        if isinstance(val, (datetime, pd.Timestamp)):
            dt_obj = val
        
        # 2. اگر رشته است، با استفاده از pandas آن را هوشمندانه پارس کن
        elif isinstance(val, str):
            # dayfirst=True باعث می‌شود در فرمت‌هایی مثل 01/02/2024، اولی را "روز" ببیند (استاندارد اروپا)
            dt_obj = pd.to_datetime(val.strip(), dayfirst=True)
            
        # 3. برای سایر موارد (مثل اعداد سریال اکسل)
        else:
            dt_obj = pd.to_datetime(val)
        
        # خروجی نهایی را به صورت Timestamp برگردان (مناسب برای انتقال به PostgreSQL)
        return dt_obj

    except Exception:
        raise DataValidationError(
            ErrorType.INVALID_DATE, 
            f"Value '{val}' is not a valid date. Please use YYYY-MM-DD or DD/MM/YYYY."
        )       

def check_pattern(val, pattern):
    """
    Checks if a value matches a given Regex pattern.
    Used for Project Codes, Customer Codes, etc.
    """
    if val is None:
        return False
    return bool(re.match(pattern, str(val)))

def validate_type(val, expected_type):
    """
    Helper to validate basic types before processing.
    """
    if val is None:
        return True
    
    if expected_type == "float":
        try:
            float(str(val).replace(',', ''))
            return True
        except ValueError:
            return False
    return True

def check_email_format(email):
    """
    Standard Email Regex Validator
    """
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if pd.isna(email) or str(email).strip() == "":
        return True # Handled by 'required' rule elsewhere
    return bool(re.match(pattern, str(email).strip()))

def validate_numeric(val, rules):
    """
    Validates numeric types and enforces range constraints (min/max).
    All internal comments are in English as requested.
    """
    if val is None or str(val).strip() == "":
        return True
    
    # Remove commas and whitespace for clean conversion
    clean_val = str(val).replace(',', '').strip()
    expected_type = rules.get('type')
    min_val = rules.get('min_value')
    max_val = rules.get('max_value')
    
    try:
        num_val = float(clean_val)
        
        # Check if the value must be a whole number (Integer)
        if expected_type == "int" and not num_val.is_integer():
            return False
            
        # Enforce minimum value constraint from YAML
        if min_val is not None:
            if num_val < min_val:
                return False 
        
        # Enforce maximum value constraint from YAML
        if max_val is not None:
            if num_val > max_val:
                return False
                
        # If all checks pass, return True
        return True
        
    except (ValueError, TypeError):
        # Return False if conversion to float fails
        return False