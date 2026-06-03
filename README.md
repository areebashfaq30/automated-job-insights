# 🤖 Automated Job Insights

An automated job market analytics pipeline that collects, cleans, and analyzes AI/ML/Data job postings from multiple sources.

## 🔧 Tech Stack
- **Apache Airflow** — pipeline orchestration
- **KNIME** — data cleaning & transformation
- **n8n** — automated notifications
- **Python** — data extraction scripts

## 📦 Data Sources
- Arbeitnow
- RemoteOK
- Himalayas
- RemoteJobs.org

## 📁 Project Structure
```
├── dags/                  # Airflow DAG
├── scripts/               # Extraction & merging scripts
├── data/
│   ├── raw/               # Raw data from each source
│   └── processed/         # Cleaned & analytics-ready data
├── n8n/                   # n8n workflow
└── knime_workflow/        # KNIME cleaning workflow
```

## 📊 Analysis Output
- Source job counts
- Remote vs on-site ratio
- Experience level brackets
- Entry-level (0–1 yr) job listings
- Average salary in USD

## 🚀 Assignment
University of Central Punjab — Tools & Techniques for DS, Assignment #3

