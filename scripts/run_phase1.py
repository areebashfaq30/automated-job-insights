"""
run_phase1.py
Run this from the job_market_project/ root:
    python scripts/run_phase1.py

It runs all 4 extraction scripts then merges them.
"""
import subprocess
import sys
import os

SCRIPTS_DIR = os.path.dirname(__file__)

scripts = [
    "extract_arbeitnow.py",
    "extract_remoteok.py",
    "extract_himalayas.py",
    "extract_remotejobs.py",
]

print("=" * 50)
print("PHASE 1 — Extraction")
print("=" * 50)

for script in scripts:
    path = os.path.join(SCRIPTS_DIR, script)
    print(f"\n>>> Running {script}...")
    result = subprocess.run([sys.executable, path], capture_output=False)
    if result.returncode != 0:
        print(f"WARNING: {script} failed with exit code {result.returncode}")

print("\n" + "=" * 50)
print("PHASE 2 — Merging")
print("=" * 50)
merge_path = os.path.join(SCRIPTS_DIR, "merge_sources.py")
subprocess.run([sys.executable, merge_path])

print("\nDone! Check data/raw/ for raw files and data/merged/ for merged_raw_jobs.csv")
