
import pandas as pd
import re

day_conversion_map = {
    "day": 1,
    "days": 1,
    "week": 7,
    "weeks": 7,
    "month": 30,
    "months": 30,
    "year": 365,
    "years": 365
}

def duration_to_days(value):
    if pd.isna(value):
        return pd.NA
    
    value = str(value).strip().lower()
    
    match = re.fullmatch(r"(\d+)\s*(day|days|week|weeks|month|months|year|years)", value)
    
    if not match:
        return pd.NA  # flag invalid
    
    number = int(match.group(1))
    unit = match.group(2)
    
    return number * day_conversion_map[unit]