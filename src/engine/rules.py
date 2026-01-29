from datetime import datetime
import pandas as pd

def validate_business_rules(rules, current_val, row):
    """
    Comprehensive Business Rule Engine.
    Handles: Length, Ranges, Allowed Lists, and Conditional Logic.
    """
    if current_val is None:
        return True, None

    # 1. LENGTH CONTROLS
    val_str = str(current_val)
    if "min_length" in rules and len(val_str) < rules["min_length"]:
        return False, f"Too short (Min: {rules['min_length']})"
    
    if "max_length" in rules and len(val_str) > rules["max_length"]:
        return False, f"Too long (Max: {rules['max_length']})"

    # 2. ALLOWED VALUES (ENUM)
    if "allowed_values" in rules:
        allowed = [str(v).lower() for v in rules["allowed_values"]]
        if str(current_val).lower() not in allowed:
            return False, f"Invalid value. Must be one of: {rules['allowed_values']}"

    # 3. DATE CONTROLS
    if isinstance(current_val, datetime):
        if "min_year" in rules and current_val.year < rules["min_year"]:
            return False, f"Year {current_val.year} is before allowed minimum {rules['min_year']}"
        
        if rules.get("max_year") == "current" or rules.get("not_future"):
            if current_val.year > datetime.now().year:
                return False, f"Future dates not allowed ({current_val.year})"

    # 4. CONDITIONAL/CUSTOM RULES
    # Supporting both 'custom_rule' and 'conditional_rules' tags for flexibility
    c_rules = rules.get("custom_rule") or rules.get("conditional_rules")
    if c_rules:
        # Wrap in list if it's a single dict
        if isinstance(c_rules, dict): c_rules = [c_rules]
        
        for condition in c_rules:
            if condition.get("type") == "conditional":
                dep_col = condition.get("if_col")
                expected = str(condition.get("equals")).lower()
                actual = str(row.get(dep_col)).strip().lower()

                if actual == expected:
                    must_be = condition.get("then_value") or condition.get("must_be")
                    if str(current_val) != str(must_be):
                        return False, f"Constraint: Since {dep_col} is {actual}, this must be {must_be}"

    return True, None