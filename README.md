# Global Development Indicator Pipeline

[![Status](https://img.shields.io/badge/Status-WIP-orange.svg)]
[![Tech](https://img.shields.io/badge/Stack-Python%20%7C%20BigQuery%20%7C%20SQL-blue.svg)]

## 📌 Project Overview
Architecting an automated ETL pipeline to ingest, transform, and fuse global development data from **World Bank**, **WFP**, and **FAOSTAT**. This project centralizes fragmented indicators into a unified Google BigQuery warehouse to drive real-time trend analysis (2010–Present) for The Hunger Project’s (THP) focus countries.

The primary goal is to bridge the gap between historical national baselines and real-time food security metrics, enabling data-driven humanitarian strategy.

## 🛠️ Technical Stack
* **Language:** Python (Data Extraction & Orchestration)
* **Data Warehouse:** Google BigQuery
* **Transformation:** SQL (Modular CTEs)
* **Visualization:** Looker Studio
* **APIs:** World Bank (Macro-economics), WFP HungerMap (Food Prices), FAOSTAT (Agriculture)

## 🏗️ Data Architecture
The pipeline follows a modular design to ensure scalability:

1.  **Ingestion:** Python scripts interface with REST APIs to extract indicators across three domains:
    * **Macro-Economics:** GDP, health spending, and stunting (World Bank).
    * **Agriculture:** Land rights and rural farmer productivity (FAOSTAT).
    * **Real-Time Metrics:** Hyper-local food price volatility (WFP).
2.  **Fusion:** Data is staged in BigQuery and joined using a composite key of `ISO3 Country Code` + `Year`.
3.  **Analytics Layer:** A refined SQL view (`thp_main_view.sql`) normalizes disparate data sources into a single source of truth for downstream BI.

## 📂 Repository Structure
```text
├── docs/               # Technical specs and dashboard wireframes
├── scripts/            # Modular Python extraction logic
│   ├── fetch_faostat.py
│   ├── fetch_wfp.py
│   └── fetch_world_bank.py
├── sql/                # Transformation logic & view definitions
│   └── thp_main_view.sql
├── requirements.txt    # Project dependencies
└── README.md

## 📊 Data Dictionary & Logic
| Source | Category | Join Key | Strategic Value |
| :--- | :--- | :--- | :--- |
| **World Bank** | Health/Macro | ISO3 + Year | Establishes national baseline (e.g., Stunting). |
| **FAOSTAT** | Agriculture | ISO3 + Year | Monitors rural land rights and food production. |
| **WFP** | Food Security | ISO3 + Year | Provides "Now" vs. "Historical" price trends. |

## 🚀 Roadmap
[ ] Automation: Migrate local scripts to GitHub Actions for weekly scheduled runs.

[ ] Resilience: Implement error handling for API rate limits and schema drifts.

[ ] Expansion: Integrate ACLED data to correlate conflict-induced instability with food price spikes.

Note: This project is a Work-In-Progress