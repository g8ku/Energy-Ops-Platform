# Energy Operations Analytics Platform

A Python + SQL analytics pipeline and interactive dashboard for analyzing operational efficiency metrics across a fleet of energy infrastructure assets (wind turbines, solar arrays, substations).

Built as a portfolio project demonstrating data engineering, SQL analytics, and business intelligence dashboard skills relevant to energy sector internship roles.

---

## What It Does

- **Generates** a realistic synthetic dataset: 24 assets, 8,760 daily sensor readings, 123 maintenance events across 12 months
- **Loads** data into a relational SQLite database with a normalized schema
- **Analyzes** operational efficiency via a Python pipeline using pandas and SQL
- **Flags** underperforming assets automatically based on configurable thresholds
- **Visualizes** all KPIs in a self-contained HTML dashboard (no server required)

---

## Project Structure

```
energy-ops-platform/
├── data/
│   ├── generate_data.py       # Synthetic dataset generator
│   ├── assets.csv
│   ├── sensor_readings.csv    # 8,760 rows of daily sensor data
│   ├── maintenance_logs.csv
│   ├── efficiency_metrics.csv
│   └── energy_ops.db          # SQLite database
│
├── sql/
│   └── schema.sql             # Table definitions + analytical views
│
├── analytics/
│   └── pipeline.py            # KPI computation, anomaly detection, reporting
│
├── dashboard/
│   └── index.html             # Interactive analytics dashboard
│
├── requirements.txt
└── README.md
```

---

## Quick Start

**1. Clone and install dependencies**

```bash
git clone https://github.com/your-username/energy-ops-platform.git
cd energy-ops-platform
pip install -r requirements.txt
```

**2. Generate the dataset and database**

```bash
python data/generate_data.py
```

**3. Run the analytics pipeline**

```bash
python analytics/pipeline.py
```

This prints fleet KPIs, flagged assets, and anomaly counts to the terminal, and writes five CSV reports to `data/`.

**4. Open the dashboard**

Open `dashboard/index.html` in any browser — no server needed.

---

## Key Metrics Tracked

| Metric | Description |
|--------|-------------|
| Fleet Efficiency | Average energy conversion efficiency across all assets (%) |
| Availability | Average uptime hours / 24h per asset |
| Capacity Factor | Actual output vs. theoretical maximum (%) |
| Maintenance Cost | Total CAD spend, broken down by event type |
| Anomaly Detection | Sensor readings exceeding vibration/temperature thresholds |

---

## Dashboard Features

- Monthly output and efficiency trend (switchable between Output MWh / Efficiency % / Availability %)
- Asset performance ranking table with status indicators
- Underperformer flag panel (assets below 78% efficiency threshold)
- Maintenance cost breakdown by event type (donut chart)
- Regional efficiency comparison (bar chart)

---

## Tech Stack

| Layer | Tools |
|-------|-------|
| Data generation | Python · NumPy · pandas |
| Storage | SQLite · SQL views |
| Analytics pipeline | pandas · sqlite3 |
| Dashboard | Chart.js · HTML/CSS/JS |

---

## Sample Pipeline Output

```
=======================================================
  Energy Operations Analytics Platform
  Run date: 2023-12-31
=======================================================

── Fleet KPIs (YTD) ──────────────────────────────────
  Total Output Mwh                   23,822.7
  Avg Fleet Efficiency                  84.83
  Avg Availability Pct                  87.43
  Total Maintenance Cost           407,492.63
  Total Assets                             24

── Underperforming Assets (efficiency < 78%) ─────────
 asset_id  asset_type       region  ytd_efficiency_pct     flag_reason
  SPV-004 Solar Array Eastern Grid               77.58 Below threshold

── Sensor Anomaly Summary ────────────────────────────
  Total anomalous readings : 500
  High-risk readings       : 113
```

---

## Extending This Project

- **Swap SQLite for PostgreSQL**: update the connection string in `analytics/pipeline.py`
- **Add real data**: replace `data/generate_data.py` with an ETL script pulling from your data source
- **Schedule the pipeline**: wrap `pipeline.py` in a cron job or Airflow DAG
- **Deploy the dashboard**: host `dashboard/index.html` on GitHub Pages or any static host

---
