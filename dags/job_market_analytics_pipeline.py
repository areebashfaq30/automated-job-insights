"""
job_market_analytics_pipeline.py
Airflow DAG for Job Market Analytics Pipeline
Assignment 3 - Tools & Techniques for DS
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os
import requests
import json
import shutil

# ─── PATHS ───────────────────────────────────────────────────────────────────
# Update this to your actual project path
# Replace all the Windows paths with:
PROJECT_DIR  = "/opt/airflow/job_market_project"
SCRIPTS_DIR  = "/opt/airflow/job_market_project"
DATA_RAW     = "/opt/airflow/job_market_project/data/raw"
DATA_MERGED  = "/opt/airflow/job_market_project/data/merged"
DATA_PROC    = "/opt/airflow/job_market_project/data/processed"
DATA_ARCHIVE = "/opt/airflow/job_market_project/data/archive"


# n8n webhook URL — update after setting up n8n
N8N_WEBHOOK = "http://host.docker.internal:5678/webhook/job-market-alert"
# ─── DEFAULT ARGS ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "sameen",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ─── DAG DEFINITION ───────────────────────────────────────────────────────────
dag = DAG(
    "job_market_analytics_pipeline",
    default_args=default_args,
    description="Job Market Analytics Pipeline - Assignment 3",
    schedule="@daily",
    start_date=datetime(2026, 5, 16),
    catchup=False,
    tags=["assignment3", "job_market"],
)

# ─── TASK FUNCTIONS ───────────────────────────────────────────────────────────

def run_extract_arbeitnow():
    print(">>> Extracting from Arbeitnow...")
    os.makedirs(DATA_RAW, exist_ok=True)
    result = subprocess.run(
        [sys.executable, os.path.join(SCRIPTS_DIR, "extract_arbeitnow.py")],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Arbeitnow extraction failed: {result.stderr}")
    print(">>> Arbeitnow extraction complete!")


def run_extract_remoteok():
    print(">>> Extracting from RemoteOK...")
    os.makedirs(DATA_RAW, exist_ok=True)
    result = subprocess.run(
        [sys.executable, os.path.join(SCRIPTS_DIR, "extract_remoteok.py")],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"RemoteOK extraction failed: {result.stderr}")
    print(">>> RemoteOK extraction complete!")


def run_extract_himalayas():
    print(">>> Extracting from Himalayas/Remotive...")
    os.makedirs(DATA_RAW, exist_ok=True)
    result = subprocess.run(
        [sys.executable, os.path.join(SCRIPTS_DIR, "extract_himalayas.py")],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Himalayas extraction failed: {result.stderr}")
    print(">>> Himalayas extraction complete!")


def run_extract_remotejobs():
    print(">>> Extracting from RemoteJobs.org...")
    os.makedirs(DATA_RAW, exist_ok=True)
    result = subprocess.run(
        [sys.executable, os.path.join(SCRIPTS_DIR, "extract_remotejobs.py")],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"RemoteJobs extraction failed: {result.stderr}")
    print(">>> RemoteJobs extraction complete!")


def run_merge_sources():
    print(">>> Merging all sources...")
    os.makedirs(DATA_MERGED, exist_ok=True)
    result = subprocess.run(
        [sys.executable, os.path.join(SCRIPTS_DIR, "merge_sources.py")],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Merge failed: {result.stderr}")

    merged_file = os.path.join(DATA_MERGED, "merged_raw_jobs.csv")
    if not os.path.exists(merged_file):
        raise Exception("merged_raw_jobs.csv not found after merge!")
    print(f">>> Merge complete! File: {merged_file}")


def run_knime_workflow():
   def run_knime_workflow():
    import requests
    response = requests.post(
        "http://host.docker.internal:8005/run-knime",
        timeout=3600
    )
    if response.status_code != 200:
        raise Exception(f"KNIME failed: {response.text}")
    print("KNIME done!")


def validate_clean_output():
    print(">>> Validating outputs...")
    import pandas as pd

    clean_file   = os.path.join(DATA_PROC, "clean_ai_ml_data_jobs.csv")
    metrics_file = os.path.join(DATA_PROC, "metrics_summary.csv")

    # Check files exist
    if not os.path.exists(clean_file):
        raise Exception(f"clean_ai_ml_data_jobs.csv not found in {DATA_PROC}")
    if not os.path.exists(metrics_file):
        raise Exception(f"metrics_summary.csv not found in {DATA_PROC}")

    # Check not empty
    df = pd.read_csv(clean_file)
    if len(df) == 0:
        raise Exception("clean_ai_ml_data_jobs.csv is empty!")

    print(f">>> Validation passed! Rows: {len(df)}, Columns: {len(df.columns)}")

    # Check required columns
    required_cols = ["source", "title", "company_name", "remote_status",
                     "experience_bracket", "job_url", "description"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"WARNING: Missing columns: {missing}")

    # Missing value check
    for col in ["title", "company_name", "job_url"]:
        if col in df.columns:
            pct = df[col].isnull().mean() * 100
            print(f"  Missing {col}: {pct:.1f}%")

    print(">>> Validation complete!")


def calculate_metrics():
    print(">>> Calculating metrics...")
    import pandas as pd

    clean_file = os.path.join(DATA_PROC, "clean_ai_ml_data_jobs.csv")
    df = pd.read_csv(clean_file)

    metrics = {}

    # Total jobs
    metrics["total_jobs"] = len(df)

    # Jobs by source
    if "source" in df.columns:
        metrics["jobs_by_source"] = df["source"].value_counts().to_dict()

    # Remote ratio
    if "remote_status" in df.columns:
        metrics["remote_ratio"] = df["remote_status"].value_counts().to_dict()

    # Experience bracket
    if "experience_bracket" in df.columns:
        metrics["experience_distribution"] = df["experience_bracket"].value_counts().to_dict()
        metrics["zero_to_one_year_jobs"] = int(df[df["experience_bracket"] == "0-1"].shape[0])

    # Average salary
    if "salary_mid_usd" in df.columns:
        salary_df = df[df["salary_mid_usd"].notna() & (df["salary_mid_usd"] > 0)]
        if len(salary_df) > 0:
            metrics["average_salary_usd"] = round(float(salary_df["salary_mid_usd"].mean()), 2)
            metrics["salary_coverage_pct"] = round(len(salary_df) / len(df) * 100, 1)
        else:
            metrics["average_salary_usd"] = None

    metrics["pipeline_status"] = "Success"
    metrics["run_date"] = str(datetime.now())

    # Save metrics
    metrics_path = os.path.join(DATA_PROC, "pipeline_metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)

    print(f">>> Metrics saved! Total jobs: {metrics['total_jobs']}")
    print(json.dumps(metrics, indent=2))
    return metrics


def trigger_n8n_workflow():
    print(">>> Triggering n8n webhook...")
    import pandas as pd

    clean_file   = os.path.join(DATA_PROC, "clean_ai_ml_data_jobs.csv")
    metrics_path = os.path.join(DATA_PROC, "pipeline_metrics.json")

    df = pd.read_csv(clean_file)

    # Load metrics
    if os.path.exists(metrics_path):
        with open(metrics_path) as f:
            metrics = json.load(f)
    else:
        metrics = {"total_jobs": len(df), "pipeline_status": "Success"}

    payload = {
        "total_jobs": metrics.get("total_jobs", len(df)),
        "jobs_by_source": metrics.get("jobs_by_source", {}),
        "remote_ratio": metrics.get("remote_ratio", {}),
        "zero_to_one_year_jobs": metrics.get("zero_to_one_year_jobs", 0),
        "average_salary_usd": metrics.get("average_salary_usd", "N/A"),
        "pipeline_status": "Success",
        "run_date": str(datetime.now()),
    }

    try:
        response = requests.post(N8N_WEBHOOK, json=payload, timeout=30)
        print(f">>> n8n response: {response.status_code}")
        print(response.text)
    except Exception as e:
        print(f"WARNING: n8n trigger failed: {e}")
        print(">>> Pipeline completed but n8n notification not sent")


def archive_outputs():
    print(">>> Archiving outputs...")
    os.makedirs(DATA_ARCHIVE, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    files_to_archive = [
        os.path.join(DATA_PROC, "clean_ai_ml_data_jobs.csv"),
        os.path.join(DATA_PROC, "metrics_summary.csv"),
        os.path.join(DATA_MERGED, "merged_raw_jobs.csv"),
    ]

    for f in files_to_archive:
        if os.path.exists(f):
            fname = os.path.basename(f)
            dest  = os.path.join(DATA_ARCHIVE, f"{timestamp}_{fname}")
            shutil.copy2(f, dest)
            print(f"  Archived: {fname}")

    print(">>> Archiving complete!")


# ─── DEFINE TASKS ─────────────────────────────────────────────────────────────

t_extract_arbeitnow = PythonOperator(
    task_id="extract_arbeitnow",
    python_callable=run_extract_arbeitnow,
    dag=dag,
)

t_extract_remoteok = PythonOperator(
    task_id="extract_remoteok",
    python_callable=run_extract_remoteok,
    dag=dag,
)

t_extract_himalayas = PythonOperator(
    task_id="extract_himalayas",
    python_callable=run_extract_himalayas,
    dag=dag,
)

t_extract_remotejobs = PythonOperator(
    task_id="extract_remotejobs",
    python_callable=run_extract_remotejobs,
    dag=dag,
)

t_merge = PythonOperator(
    task_id="merge_sources",
    python_callable=run_merge_sources,
    dag=dag,
)

t_knime = PythonOperator(
    task_id="run_knime_workflow",
    python_callable=run_knime_workflow,
    dag=dag,
)

t_validate = PythonOperator(
    task_id="validate_clean_output",
    python_callable=validate_clean_output,
    dag=dag,
)

t_metrics = PythonOperator(
    task_id="calculate_metrics",
    python_callable=calculate_metrics,
    dag=dag,
)

t_n8n = PythonOperator(
    task_id="trigger_n8n_workflow",
    python_callable=trigger_n8n_workflow,
    dag=dag,
)

t_archive = PythonOperator(
    task_id="archive_outputs",
    python_callable=archive_outputs,
    dag=dag,
)

# ─── TASK DEPENDENCIES ────────────────────────────────────────────────────────
# 4 extractions run in parallel, then merge, then rest in sequence

[t_extract_arbeitnow, t_extract_remoteok, t_extract_himalayas, t_extract_remotejobs] >> t_merge
t_merge >> t_knime >> t_validate >> t_metrics >> t_n8n >> t_archive
