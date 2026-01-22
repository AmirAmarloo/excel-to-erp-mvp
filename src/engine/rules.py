from datetime import datetime
import pytz

def validate_business_rules(rules, current_val, row):
    """
    Validates direct rules from YAML like min_length, max_year, and allowed_values.
    """
    # 1. String Length Checks
    if rules.get("type") == "string":
        val_str = str(current_val)
        if "min_length" in rules and len(val_str) < rules["min_length"]:
            return False, f"Length less than {rules['min_length']}"
        if "max_length" in rules and len(val_str) > rules["max_length"]:
            return False, f"Length more than {rules['max_length']}"

    # 2. Allowed Values (Enum)
    if "allowed_values" in rules:
        allowed = [str(v).lower() for v in rules["allowed_values"]]
        if str(current_val).lower() not in allowed:
            return False, f"Value not in allowed list: {rules['allowed_values']}"

    # 3. Date Range Checks
    if rules.get("type") == "datetime" and isinstance(current_val, datetime):
        min_y = rules.get("min_year")
        max_y = datetime.now().year if rules.get("max_year") == "current" else rules.get("max_year")
        
        if min_y and current_val.year < min_y:
            return False, f"Year earlier than {min_y}"
        if max_y and current_val.year > max_y:
            return False, f"Year later than {max_y}"
        if rules.get("not_future"):
            now = datetime.now(pytz.UTC)
            val_utc = current_val if current_val.tzinfo else pytz.UTC.localize(current_val)
            if val_utc > now:
                return False, "Date is in the future"

    # 4. Conditional Rules (Suspend Logic)
    if "custom_rule" in rules:
        c_rule = rules["custom_rule"]
        if c_rule.get("type") == "conditional":
            if str(row.get(c_rule["if_col"])).lower() == str(c_rule["equals"]).lower():
                if float(current_val) != float(c_rule["then_value"]):
                    return False, f"Amount must be {c_rule['then_value']} when {c_rule['if_col']} is {c_rule['equals']}"

    return True, None
