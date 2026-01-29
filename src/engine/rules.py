from datetime import datetime
import pandas as pd

def validate_business_rules(rules, current_val, row):
    # If the value is missing and not required, we don't apply business rules
    if pd.isna(current_val) or current_val == "":
        return True, None

    val_str = str(current_val).strip()

    # 1. LENGTH CONTROLS
    if "min_length" in rules and len(val_str) < rules["min_length"]:
        return False, f"Too short (Min: {rules['min_length']})"
    
    if "max_length" in rules and len(val_str) > rules["max_length"]:
        return False, f"Too long (Max: {rules['max_length']})"

    # 2. ALLOWED VALUES (Safe Check)
    if "allowed_values" in rules:
        allowed = [str(v).lower() for v in rules["allowed_values"]]
        if val_str.lower() not in allowed:
            return False, f"Invalid value. Must be one of: {rules['allowed_values']}"

    # 3. DATE CONTROLS (Safe Conversion)
    if isinstance(current_val, datetime):
        if "min_year" in rules and current_val.year < rules["min_year"]:
            return False, f"Year {current_val.year} is before {rules['min_year']}"
        if rules.get("not_future") and current_val.year > datetime.now().year:
            return False, f"Future dates not allowed ({current_val.year})"

    # 4. CONDITIONAL RULES (Safe Lower)
    c_rules = rules.get("custom_rule") or rules.get("conditional_rules")
    if c_rules:
        if isinstance(c_rules, dict): c_rules = [c_rules]
        for cond in c_rules:
            dep_col = cond.get("if_col")
            # Safe way to get and lower the dependent value
            actual_val = str(row.get(dep_col, "")).strip().lower()
            expected = str(cond.get("equals", "")).lower()

            if actual_val == expected:
                must_be = cond.get("then_value") or cond.get("must_be")
                if val_str != str(must_be):
                    return False, f"Constraint: Since {dep_col} is {actual_val}, this must be {must_be}"

    return True, None