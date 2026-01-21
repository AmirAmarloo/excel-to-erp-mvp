from datetime import datetime
import pytz

def validate_custom_rule(rule, current_val, row):
    """
    Final Rule Engine supporting all MVP requirements.
    """
    rule_type = rule.get("type")
    
    # 1. Length Validation
    if rule_type == "length":
        return rule.get("min", 0) <= len(str(current_val)) <= rule.get("max", 999)

    # 2. Status Membership
    elif rule_type == "allowed_values":
        return str(current_val).lower() in [v.lower() for v in rule.get("list", [])]

    # 3. Conditional Rule (Suspend Logic)
    elif rule_type == "conditional":
        if str(row.get(rule.get("if_col"))).lower() == str(rule.get("equals")).lower():
            return float(current_val) == float(rule.get("then_value"))
        return True

    # 4. Smart Date Range (Fixes the 4654654.23 issue)
    elif rule_type == "date_range":
        if not isinstance(current_val, datetime): return False
        if "min_year" in rule and current_val.year < rule["min_year"]: return False
        if "max_year" in rule and current_val.year > rule["max_year"]: return False
        if rule.get("not_future"):
            now = datetime.now(pytz.UTC)
            val_utc = current_val if current_val.tzinfo else pytz.UTC.localize(current_val)
            if val_utc > now: return False
        return True

    return True
