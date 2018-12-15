import re


def permissive_numeric_parse(s):
    try:
        return float(re.sub('[^0-9.-]', '', s))
    except:
        return None
