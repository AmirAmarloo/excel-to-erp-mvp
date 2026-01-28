from datetime import datetime
import pandas as pd

def validate_business_rules(rules, current_val, row):
    """
    Evaluates complex business constraints defined in YAML.
    Returns: (is_valid: bool, message: str)
    """
    
    # --- 1. MINIMUM LENGTH CHECK ---
    if "min_length" in rules and current_val is not None:
        if len(str(current_val)) < rules["min_length"]:
            return False, f"Value too short. Minimum length is {rules['min_length']}."

    # --- 2. DYNAMIC YEAR CHECK (max_year: current) ---
    if "max_year" in rules and isinstance(current_val, datetime):
        max_year_rule = rules["max_year"]
        target_year = datetime.now().year if max_year_rule == "current" else max_year_rule
        
        if current_val.year > target_year:
            return False, f"Year {current_val.year} exceeds the maximum allowed year ({target_year})."

    # --- 3. CONDITIONAL RULES (e.g., Suspend Logic) ---
    # Example: If status is 'suspend', the amount must be 0
    if "conditional_rules" in rules:
        for condition in rules["conditional_rules"]:
            dependent_col = condition.get("if_col")
            expected_val = condition.get("equals")
            
            # Check if the condition is met
            actual_val = str(row.get(dependent_col)).strip().lower()
            if actual_val == str(expected_val).lower():
                # Apply the constraint (e.g., must_be: 0)
                if "must_be" in condition:
                    if str(current_val) != str(condition["must_be"]):
                        return False, f"Constraint violation: {dependent_col} is {actual_val}, so this field must be {condition['must_be']}."

    return True, None
