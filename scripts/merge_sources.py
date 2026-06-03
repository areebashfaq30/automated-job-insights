import pandas as pd
import os
import re
import requests
from datetime import date

RAW_DIR = os.path.join(os.path.dirname(__file__), "data/raw")
MERGED_DIR = os.path.join(os.path.dirname(__file__), "data/merged")
OUTPUT_PATH = os.path.join(MERGED_DIR, "merged_raw_jobs.csv")

RAW_FILES = {
    "Arbeitnow":     "raw_arbeitnow_jobs.csv",
    "RemoteOK":      "raw_remoteok_jobs.csv",
    "Himalayas":     "raw_himalayas_jobs.csv",
    "RemoteJobs.org":"raw_remotejobs_jobs.csv",
}

FINAL_COLUMNS = [
    "source", "job_id", "title", "company_name", "location_raw",
    "remote_status", "job_type", "category_raw", "tags_raw",
    "description", "publication_date", "job_url",
    "salary_text_raw", "salary_min_raw", "salary_max_raw",
    "currency_raw", "scrape_date"
]

def get_fx_rate(from_currency, to_currency="USD"):
    try:
        url = f"https://api.frankfurter.dev/v2/latest?base={from_currency}&symbols={to_currency}"
        r = requests.get(url, timeout=10)
        data = r.json()
        rate = data["rates"][to_currency]
        conv_date = data.get("date", str(date.today()))
        print(f"  Fetched {from_currency} → {to_currency}: {rate} (date: {conv_date})")
        return rate, conv_date
    except Exception as e:
        print(f"  WARNING: Could not fetch rate: {e}. Using 1.0 fallback.")
        return 1.0, str(date.today())

def fetch_all_fx_rates():
    print("\n=== Fetching FX rates from Frankfurter API ===")
    eur_rate, conv_date = get_fx_rate("EUR")
    gbp_rate, _ = get_fx_rate("GBP")
    return {
        "USD": (1.0, conv_date),
        "EUR": (eur_rate, conv_date),
        "GBP": (gbp_rate, conv_date),
    }, conv_date

def apply_fx(currency, fx_rates):
    if not isinstance(currency, str):
        return 1.0
    currency = currency.strip().upper()
    if currency in fx_rates:
        return fx_rates[currency][0]
    return 1.0

def load_raw_files():
    frames = []
    for source, filename in RAW_FILES.items():
        path = os.path.join(RAW_DIR, filename)
        if not os.path.exists(path):
            print(f"WARNING: {filename} not found — skipping {source}.")
            continue
        df = pd.read_csv(path)
        print(f"Loaded {len(df)} rows from {filename}")
        frames.append(df)
    return frames

def clean_description(text):
    if not isinstance(text, str):
        return ""
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def standardize_remote_status(val):
    if not isinstance(val, str):
        return "Unknown"
    val = val.strip().lower()
    if val in ("remote", "true", "1", "yes"):
        return "Remote"
    if val in ("hybrid",):
        return "Hybrid"
    if val in ("on-site", "onsite", "office", "false", "0", "no"):
        return "On-site"
    return "Unknown"

def standardize_job_type(val):
    if not isinstance(val, str):
        return "Unknown"
    val = val.strip().lower()
    if "full" in val:
        return "Full-time"
    if "part" in val:
        return "Part-time"
    if "contract" in val:
        return "Contract"
    if "freelance" in val:
        return "Freelance"
    if "intern" in val:
        return "Internship"
    return "Unknown"

def merge_and_clean(frames, fx_rates, conv_date):
    df = pd.concat(frames, ignore_index=True)
    print(f"\nTotal rows before processing: {len(df)}")

    for col in FINAL_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    df = df[FINAL_COLUMNS].copy()

    df["title"] = df["title"].fillna("").str.strip()
    df["company_name"] = df["company_name"].fillna("").str.strip()
    df["description"] = df["description"].apply(clean_description)
    df["remote_status"] = df["remote_status"].apply(standardize_remote_status)
    df["job_type"] = df["job_type"].apply(standardize_job_type)

    before = len(df)
    df = df[df["title"].str.len() > 0]
    df = df[df["company_name"].str.len() > 0]
    print(f"Dropped {before - len(df)} rows with empty title/company")

    before = len(df)
    df_with_url = df[df["job_url"].fillna("").str.len() > 0]
    df_without_url = df[df["job_url"].fillna("").str.len() == 0]
    df_with_url = df_with_url.drop_duplicates(subset=["job_url"])
    df_without_url = df_without_url.drop_duplicates(subset=["title", "company_name", "source"])
    df = pd.concat([df_with_url, df_without_url], ignore_index=True)
    print(f"Removed {before - len(df)} duplicates. Remaining: {len(df)}")

    df["fx_rate_to_usd"] = df["currency_raw"].apply(lambda c: apply_fx(c, fx_rates))
    df["conversion_date"] = conv_date

    return df

def print_source_counts(df):
    print("\n=== Source Counts ===")
    for src, count in df["source"].value_counts().items():
        print(f"  {src}: {count} jobs")

def main():
    print("=== Merging all raw sources ===")
    frames = load_raw_files()

    if not frames:
        print("No raw files found. Run extraction scripts first.")
        return

    fx_rates, conv_date = fetch_all_fx_rates()
    df = merge_and_clean(frames, fx_rates, conv_date)
    print_source_counts(df)

    os.makedirs(MERGED_DIR, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"\nMerged dataset saved to: {OUTPUT_PATH}")
    print(f"Total rows: {len(df)} | Columns: {len(df.columns)}")
    print(f"FX rates — EUR→USD: {fx_rates['EUR'][0]}, GBP→USD: {fx_rates['GBP'][0]}")
    print(f"Conversion date: {conv_date}")

if __name__ == "__main__":
    main()