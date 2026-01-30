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

    try:
        # If it's already a datetime object (from Excel)
        if isinstance(val, datetime):
            dt_obj = val
        # If it's a string, try to parse it
        elif isinstance(val, str):
            # Supports standard format: YYYY-MM-DD
            dt_obj = datetime.strptime(val.strip(), "%Y-%m-%d")
        else:
            # For any other types like Excel serial numbers
            dt_obj = pd.to_datetime(val)
        
        return dt_obj
    except Exception:
        raise DataValidationError(
            ErrorType.INVALID_DATE, 
            f"Value '{val}' is not a valid date (Expected YYYY-MM-DD)."
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