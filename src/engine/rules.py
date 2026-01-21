from datetime import datetime
import pytz

def validate_custom_rule(rule, current_val, row):
    """
    Complete Rule Engine supporting length, allowed_values, 
    conditional (suspend case), and date_range.
    """
    rule_type = rule.get("type")
    
    # 1. String Length Check
    if rule_type == "length":
        val_len = len(str(current_val))
        return rule.get("min", 0) <= val_len <= rule.get("max", 999)

    # 2. Allowed Membership Check (Status)
    elif rule_type == "allowed_values":
        allowed_list = [str(v).lower() for v in rule.get("list", [])]
        return str(current_val).lower() in allowed_list

    # 3. Conditional Rule (Amount vs Status)
    elif rule_type == "conditional":
        if_col = rule.get("if_col")
        # If the condition (status == suspend) is met
        if str(row.get(if_col)).lower() == str(rule.get("equals")).lower():
            # Current value (amount) must match the then_value (0)
            return float(current_val) == float(rule.get("then_value"))
        return True # If not suspend, amount can be anything

    # 4. Date Range & Future Check
    elif rule_type == "date_range":
        if not isinstance(current_val, datetime):
            return False
        
        if "min_year" in rule and current_val.year < rule["min_year"]:
            return False
        if "max_year" in rule and current_val.year > rule["max_year"]:
            return False
            
        if rule.get("not_future"):
            now = datetime.now(pytz.UTC)
            val_utc = current_val if current_val.tzinfo else pytz.UTC.localize(current_val)
            if val_utc > now:
                return False
        return True

    return True
