import requests
import pandas as pd
from datetime import date
import os

SOURCE_NAME = "RemoteOK"
API_URL = "https://remoteok.com/api"
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "data/raw/raw_remoteok_jobs.csv")

def fetch_remoteok():
    try:
        headers = {"User-Agent": "Mozilla/5.0 (academic research project)"}
        response = requests.get(API_URL, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        # First item is metadata, skip it
        jobs = [j for j in data if isinstance(j, dict) and j.get("id")]
        print(f"Fetched {len(jobs)} jobs from RemoteOK")
        return jobs
    except requests.RequestException as e:
        print(f"Error fetching RemoteOK: {e}")
        return []

def standardize(jobs):
    rows = []
    for job in jobs:
        salary_min = job.get("salary_min") or None
        salary_max = job.get("salary_max") or None

        # Convert 0 to None
        if salary_min == 0:
            salary_min = None
        if salary_max == 0:
            salary_max = None

        rows.append({
            "source": SOURCE_NAME,
            "job_id": str(job.get("id", "")),
            "title": job.get("position", ""),
            "company_name": job.get("company", ""),
            "location_raw": job.get("location", "Worldwide"),
            "remote_status": "Remote",  # RemoteOK is remote-only
            "job_type": "Full-time",    # RemoteOK default
            "category_raw": "",
            "tags_raw": ", ".join(job.get("tags", [])),
            "description": job.get("description", ""),
            "publication_date": job.get("date", ""),
            "job_url": job.get("url", job.get("apply_url", "")),
            "salary_text_raw": "",
            "salary_min_raw": salary_min,
            "salary_max_raw": salary_max,
            "currency_raw": "USD" if (salary_min or salary_max) else "",
            "scrape_date": str(date.today()),
        })
    return rows

def main():
    print(f"=== Extracting from {SOURCE_NAME} ===")
    jobs = fetch_remoteok()

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
