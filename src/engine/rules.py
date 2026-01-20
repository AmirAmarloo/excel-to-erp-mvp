from datetime import datetime
import pytz

def validate_custom_rule(rule, current_val, row):
    """DSL-based rule engine to avoid insecure eval()."""
    rule_type = rule.get("type")
    
    if rule_type == "conditional":
        if_col = rule.get("if_col")
        if row.get(if_col) == rule.get("equals"):
            return current_val == rule.get("then_value")
        return True

    elif rule_type == "compare":
        op = rule.get("operator")
        right_side = rule.get("right")
        
        # Determine comparison target
        if right_side == "now":
            # Ensure comparison is offset-aware for UTC
            compare_to = datetime.now(pytz.UTC)
        else:
            compare_to = right_side
        
        if op == "<=": return current_val <= compare_to
        if op == ">=": return current_val >= compare_to
        if op == "==": return current_val == compare_to
        if op == "!=": return current_val != compare_to
        
    return True
