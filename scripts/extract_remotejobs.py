import requests
import pandas as pd
from datetime import date
import time
import os

SOURCE_NAME = "RemoteJobs.org"
API_URL = "https://remotejobs.org/api/v1/jobs"
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "data/raw/raw_remotejobs_jobs.csv")

CATEGORIES = ["data-science", "software-dev", "devops-sysadmin"]

def fetch_remotejobs(limit=50, max_pages=5):
    all_jobs = []

    for category in CATEGORIES:
        page = 1
        while page <= max_pages:
            try:
                params = {"category": category, "limit": limit, "page": page}
                response = requests.get(API_URL, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()

                # Handle both list response and dict with jobs key
                if isinstance(data, list):
                    jobs = data
                elif isinstance(data, dict):
                    jobs = data.get("jobs", data.get("data", []))
                else:
                    jobs = []

                if not jobs:
                    break

                all_jobs.extend(jobs)
                print(f"Category '{category}' page {page}: {len(jobs)} jobs (total: {len(all_jobs)})")
                page += 1
                time.sleep(0.5)

            except requests.RequestException as e:
                print(f"Error (category={category}, page={page}): {e}")
                break

    # Deduplicate
    seen = set()
    unique = []
    for job in all_jobs:
        jid = job.get("id") or job.get("url")
        if jid and jid not in seen:
            seen.add(jid)
            unique.append(job)

    print(f"Unique jobs after dedup: {len(unique)}")
    return unique

def standardize(jobs):
    rows = []
    for job in jobs:
        salary_min = job.get("salary_min") or None
        salary_max = job.get("salary_max") or None
        salary_text = job.get("salary_text", "") or ""

        if salary_min == 0:
            salary_min = None
        if salary_max == 0:
            salary_max = None

        company = job.get("company", {})
        company_name = company.get("name", "") if isinstance(company, dict) else str(company)

        rows.append({
            "source": SOURCE_NAME,
            "job_id": str(job.get("id", "")),
            "title": job.get("title", ""),
            "company_name": company_name,
            "location_raw": job.get("location", "Remote"),
            "remote_status": "Remote",  # RemoteJobs.org is remote-only
            "job_type": job.get("type", "Unknown"),
            "category_raw": job.get("category", ""),
            "tags_raw": ", ".join(job.get("tags", [])) if isinstance(job.get("tags"), list) else "",
            "description": job.get("description", ""),
            "publication_date": job.get("posted_at", ""),
            "job_url": job.get("url", job.get("apply_url", "")),
            "salary_text_raw": salary_text,
            "salary_min_raw": salary_min,
            "salary_max_raw": salary_max,
            "currency_raw": "USD" if (salary_min or salary_max or salary_text) else "",
            "scrape_date": str(date.today()),
        })
    return rows

def main():
    print(f"=== Extracting from {SOURCE_NAME} ===")
    jobs = fetch_remotejobs()

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
