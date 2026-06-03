# Job Market Analytics Pipeline
**T&T Assignment 3 — BSDS, UCP**

## Team
- Sameen Khan (Phase 1 & 2 — Extraction + Merge)
- Areeb Ashfaq (Phase 3 — KNIME Cleaning)
- Khadija Manzoor (Phase 4 — Airflow DAG)
- Aqsa Ilyas (Phase 5 & 6 — n8n + Report)

---

## Setup

```bash
pip install requests pandas
```

---

## Phase 1 & 2 — Run Extraction + Merge (Sameen)

```bash
# From project root
python scripts/run_phase1.py
```

This will:
1. Extract jobs from Arbeitnow, RemoteOK, Himalayas, RemoteJobs.org
2. Save 4 raw CSVs to `data/raw/`
3. Merge and standardize into `data/merged/merged_raw_jobs.csv`

---

## Phase 3 — KNIME Cleaning (Areeb)
- Import `data/merged/merged_raw_jobs.csv` into KNIME
- Apply cleaning, filtering, salary, experience bracket steps
- Export to `data/processed/clean_ai_ml_data_jobs.csv`
- Export to `data/processed/metrics_summary.csv`

---

## Phase 4 — Airflow DAG (Khadija)
- Install Airflow: `pip install apache-airflow`
- Copy `dags/job_market_analytics_pipeline.py` to your Airflow dags folder
- Run: `airflow dags trigger job_market_analytics_pipeline`

---

## Phase 5 — n8n (Aqsa)
- Import `n8n/job_market_alert_workflow.json` into n8n
- Set webhook URL in Airflow DAG config

---

## Project Structure

```
job_market_project/
  dags/                        ← Airflow DAG (Khadija)
  scripts/
    extract_arbeitnow.py       ← Sameen
    extract_remoteok.py        ← Sameen
    extract_himalayas.py       ← Sameen
    extract_remotejobs.py      ← Sameen
    merge_sources.py           ← Sameen
    validate_outputs.py        ← Khadija
    calculate_metrics.py       ← Aqsa
  data/
    raw/                       ← 4 raw CSVs
    merged/                    ← merged_raw_jobs.csv
    processed/                 ← clean_ai_ml_data_jobs.csv
    archive/
  knime_workflow/              ← Areeb
  n8n/                         ← Aqsa
  report/                      ← Aqsa
  screenshots/
```
