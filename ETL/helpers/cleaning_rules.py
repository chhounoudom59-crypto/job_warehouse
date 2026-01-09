# cleaning_rules.py
import re
import unicodedata
from datetime import datetime
from typing import Dict, Optional, Tuple

import pandas as pd

CURRENCY_RATES = {
    "USD": 1.0,
    "KHR": 0.00024,
}
SALARY_RANGE_PATTERN = re.compile(
    r"(?P<currency>[A-Za-z$]{0,3})\s*(?P<min>[\d,\.]+)\s*[-–]\s*(?P<max>[\d,\.]+)",
    re.UNICODE,
)
SALARY_SINGLE_PATTERN = re.compile(
    r"(?P<currency>[A-Za-z$]{0,3})\s*(?P<value>[\d,\.]+)",
    re.UNICODE,
)

def normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    value = unicodedata.normalize("NFKC", value)
    value = value.replace(" ", " ").strip()
    return re.sub(r"\s+", " ", value)

def standardize_company(name: str) -> str:
    value = normalize_text(name).lower()
    replacements = {
        "co., ltd": "",
        "co.,ltd": "",
        "co. ltd": "",
        "ltd.": "",
        "plc.": "",
        "plc": "",
        "inc.": "",
        "(cambodia)": "",
        "(kh)": "",
    }
    for k, v in replacements.items():
        value = value.replace(k, v)
    return normalize_text(value).title()

def standardize_job_title(title: str) -> Tuple[str, str]:
    title_norm = normalize_text(title)
    mapping: Dict[str, str] = {
        "ui/ux": "UI/UX Designer",
        "ux": "UX Designer",
        "developer": "Software Developer",
        "engineer": "Engineer",
        "officer": "Officer",
        "manager": "Manager",
    }
    title_std = title_norm
    for keyword, replacement in mapping.items():
        if keyword.lower() in title_norm.lower():
            title_std = replacement
            break
    return title_norm, title_std

def parse_salary(
    salary_raw: str,
    default_currency: str = "USD",
    keywords: Optional[Dict[str, list]] = None,
) -> Tuple[Optional[float], Optional[float], str, str]:
    if not salary_raw:
        return None, None, default_currency, "undisclosed"
    text = normalize_text(salary_raw)
    salary_type = "range"
    currency = default_currency

    if keywords:
        for label, patterns in keywords.items():
            if any(p.lower() in text.lower() for p in patterns):
                return None, None, default_currency, label

    match = SALARY_RANGE_PATTERN.search(text)
    if match:
        currency = match.group("currency") or default_currency
        salary_min = float(match.group("min").replace(",", ""))
        salary_max = float(match.group("max").replace(",", ""))
    else:
        match = SALARY_SINGLE_PATTERN.search(text)
        if match:
            currency = match.group("currency") or default_currency
            salary_min = salary_max = float(match.group("value").replace(",", ""))
            salary_type = "fixed"
        else:
            return None, None, default_currency, "undisclosed"

    currency = currency.upper().replace("$", "USD")
    rate = CURRENCY_RATES.get(currency, 1.0)
    salary_min_usd = salary_min * rate
    salary_max_usd = salary_max * rate
    return salary_min_usd, salary_max_usd, currency, salary_type

def standardize_location(value: str) -> Tuple[str, Optional[str]]:
    text = normalize_text(value)
    if "," in text:
        city, province = [part.strip() for part in text.split(",", 1)]
        return province.title(), city.title()
    return text.title(), None

def parse_date(value: str) -> Optional[datetime]:
    for fmt in ("%m/%d/%Y", "%m/%d/%Y %H:%M:%S", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None

def hash_record(fields: Tuple[str, ...]) -> str:
    import hashlib
    concatenated = "||".join(normalize_text(f) for f in fields)
    return hashlib.sha256(concatenated.encode("utf-8")).hexdigest()

def normalize_experience(value: str) -> str:
    text = normalize_text(value).lower()
    mapping = {
        "no experience": "Entry Level (0–1 year)",
        "entry": "Entry Level (0–1 year)",
        "junior": "Junior Level (1–2 years)",
        "mid": "Mid Level (2–5 years)",
        "senior": "Senior Level (5+ years)",
    }
    for keyword, canonical in mapping.items():
        if keyword in text:
            return canonical
    return normalize_text(value)

def normalize_education(value: str) -> str:
    text = normalize_text(value)
    return text if text else "Not Specified"

def transform_dataframe(df: pd.DataFrame, config: Dict) -> pd.DataFrame:
    df = df.copy()
    df["job_title_raw"], df["job_title_std"] = zip(
        *df["Job Title"].apply(standardize_job_title)
    )
    df["company_name_raw"] = df["Company Name"].apply(normalize_text)
    df["company_name_std"] = df["Company Name"].apply(standardize_company)
    df["industry_raw"] = df["Industry"].apply(normalize_text)
    df["industry_std"] = df["Industry"].apply(str.title)

    df["province"], df["city"] = zip(*df["Location"].apply(standardize_location))
    salary_results = df["Salary"].apply(
        lambda s: parse_salary(
            s,
            config["cleaning"]["currency_default"],
            config["cleaning"]["salary_keywords"],
        )
    )
    df["salary_min_usd"], df["salary_max_usd"], df["salary_currency"], df["salary_type"] = zip(*salary_results)

    df["employment_type"] = df["Job Type"].apply(normalize_text)
    df["experience_level_std"] = df["Experience Level"].apply(normalize_experience)
    df["education_level_std"] = df["Education Level"].apply(normalize_education)
    df["posting_date"] = df["Posting Date"].apply(parse_date)

    df["source_agency_raw"] = df["Source Agency"].apply(normalize_text)
    df["source_agency_std"] = df["Source Agency"].str.title()

    df["hash_dedup"] = df.apply(
        lambda row: hash_record(
            (
                row["job_title_std"],
                row["company_name_std"],
                row["province"],
                row["posting_date"].strftime("%Y-%m-%d") if row["posting_date"] else "",
            )
        ),
        axis=1,
    )
    return df