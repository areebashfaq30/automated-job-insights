import requests
import pandas as pd
from datetime import date
import time
import os

SOURCE_NAME = "Arbeitnow"
API_URL = "https://www.arbeitnow.com/api/job-board-api"
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "data/raw/raw_arbeitnow_jobs.csv")

def fetch_arbeitnow(max_pages=5):
    all_jobs = []
    page = 1

    while page <= max_pages:
        try:
            response = requests.get(API_URL, params={"page": page}, timeout=10)
            response.raise_for_status()
            data = response.json()
            jobs = data.get("data", [])

            if not jobs:
                print(f"No more jobs at page {page}. Stopping.")
                break

            all_jobs.extend(jobs)
            print(f"Page {page}: fetched {len(jobs)} jobs (total so far: {len(all_jobs)})")
            page += 1
            time.sleep(0.5)  # be respectful

        except requests.RequestException as e:
            print(f"Error on page {page}: {e}")
            break

    return all_jobs

def standardize(jobs):
    rows = []
    for job in jobs:
        rows.append({
            "source": SOURCE_NAME,
            "job_id": job.get("slug", ""),
            "title": job.get("title", ""),
            "company_name": job.get("company_name", ""),
            "location_raw": job.get("location", ""),
            "remote_status": "Remote" if job.get("remote", False) else "On-site",
            "job_type": job.get("job_types", ["Unknown"])[0] if job.get("job_types") else "Unknown",
            "category_raw": "",
            "tags_raw": ", ".join(job.get("tags", [])),
            "description": job.get("description", ""),
            "publication_date": job.get("created_at", ""),
            "job_url": job.get("url", ""),
            "salary_text_raw": "",
            "salary_min_raw": None,
            "salary_max_raw": None,
            "currency_raw": "",
            "scrape_date": str(date.today()),
        })
    return rows

def main():
    print(f"=== Extracting from {SOURCE_NAME} ===")
    jobs = fetch_arbeitnow()

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
