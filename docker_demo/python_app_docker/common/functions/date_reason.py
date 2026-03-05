
import re

def date_reason(s: str) -> str:
    if s is None:
        return "Missing"
    s = str(s).strip()
    if s == "":
        return "Missing"
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", s)
    if not m:
        return "Unexpected format"
    d, mo, y = map(int, m.groups())
    if mo < 1 or mo > 12:
        return "Month out of range"
    if d < 1 or d > 31:
        return "Day out of range"
    return "Invalid date (e.g., day/month mismatch)"