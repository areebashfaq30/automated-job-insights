"""
extract_himalayas.py
Originally targeted Himalayas.app — replaced with Remotive.com as backup
source per assignment instructions (Himalayas blocks automated requests).
Remotive is listed as an approved backup in Assignment 3.
API docs: https://remotive.com/remote-jobs/api
"""
import requests
import pandas as pd
from datetime import date
import time
import os

SOURCE_NAME = "Remotive"
API_URL = "https://remotive.com/api/remote-jobs"
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "data/raw/raw_himalayas_jobs.csv")

# AI/ML/Data categories supported by Remotive
CATEGORIES = [
    "software-dev",
    "data",
    "machine-learning",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

def fetch_remotive():
    all_jobs = []

    for category in CATEGORIES:
        try:
            params = {"category": category, "limit": 100}
            response = requests.get(API_URL, params=params, headers=HEADERS, timeout=15)
            response.raise_for_status()
            data = response.json()
            jobs = data.get("jobs", [])
            print(f"Category '{category}': {len(jobs)} jobs fetched")
            all_jobs.extend(jobs)
            time.sleep(0.5)
        except requests.RequestException as e:
            print(f"Error (category={category}): {e}")

    # Deduplicate by job id
    seen = set()
    unique = []
    for job in all_jobs:
        jid = job.get("id")
        if jid and jid not in seen:
            seen.add(jid)
            unique.append(job)

    print(f"Unique jobs after dedup: {len(unique)}")
    return unique

def standardize(jobs):
    rows = []
    for job in jobs:
        tags = job.get("tags", [])
        tags_raw = ", ".join(tags) if isinstance(tags, list) else str(tags)

        rows.append({
            "source": SOURCE_NAME,
            "job_id": str(job.get("id", "")),
            "title": job.get("title", ""),
            "company_name": job.get("company_name", ""),
            "location_raw": job.get("candidate_required_location", "Worldwide"),
            "remote_status": "Remote",  # Remotive is remote-only
            "job_type": job.get("job_type", "full_time"),
            "category_raw": job.get("category", ""),
            "tags_raw": tags_raw,
            "description": job.get("description", ""),
            "publication_date": job.get("publication_date", ""),
            "job_url": job.get("url", ""),
            "salary_text_raw": job.get("salary", ""),
            "salary_min_raw": None,
            "salary_max_raw": None,
            "currency_raw": "",
            "scrape_date": str(date.today()),
        })
    return rows

def main():
    print(f"=== Extracting from {SOURCE_NAME} (replacing Himalayas) ===")
    jobs = fetch_remotive()

    if not jobs:
        print("No jobs fetched. Check API availability.")
        return

    rows = standardize(jobs)
    df = pd.DataFrame(rows)
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Saved {len(df)} jobs to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
