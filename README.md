# NYC Elevator ETL Pipeline and Complaint Analysis

This project analyzes elevator complaints across New York City using the official NYC Open Data API. It includes a fully reproducible ETL pipeline, data cleaning, and geospatial analytics.

# Business Problem
## Business Context
Building maintenance companies in NYC need to identify high-density complaint areas to optimize service coverage and reduce response times. This analysis provides data-driven insights for strategic placement of repair hubs.

Key Questions
- What is the total elevator complaint in 2024?
- What is the average number of days taken to resolve elevator complaints in 2024?
- What is the total elevator complaint per borough and percentage?
- What is the average time taken to update the complaint created?
- What is the yearly & monthly & quarterly trends in elevator complaints?
- Which boroughs/ZIP codes have the most complaints?
- How long it takes to resolve complaints?
- What are the other highly reported service requests for expansion opportunities for the business?
- What is a suitable place/location for the headquarter (HQ)?

---

## Tech Stack
- Python
- DuckDB
- SQL
- Socrata API / SODA
- Pandas, NumPy, Matplotlib
- Docker

---

## Pipeline Steps
1. Fetch elevator complaints from NYC Open Data using API
2. Normalize and clean 50+ messy system-generated columns
3. Store results in DuckDB
4. Compute feature: `closed_in_days` (days to resolve a complaint)
5. Export data for analytics & geospatial work

---

## 📁 Project Structure
```
nyc-elevator-etl/
│
├── pipeline/
│   ├── __init__.py
│   ├── etl.py
│   ├── export_parquet.py
│   └── geo_analysis.py
│
├── sql/
│   ├── avg_closing_days.sql
│   ├── complaints_by_borough.sql
│   ├── complaints_by_month.sql
│   ├── complaints_by_quarter.sql
│   ├── resolution_time_hours.sql
│   └── total_complaints.sql
│
├── insights/
│   └── elevator_complaint_insights.md
│
├── docker/
│   └── Dockerfile
│
├── data/                # ignored by git
├── .env                 # ignored by git
├── .gitignore
├── requirements.txt
└── README.md
```

---
## Run Locally
```bash
pip install -r requirements.txt
python pipeline/etl.py

Run with Docker
```bash
docker build -t elevator-etl .
docker run --env-file .env elevator-etl

Outputs
clean_elevator_2024.csv — cleaned dataset

clean_elevator_2024.parquet — Parquet version for analytics

manhattan_elevator_heatmap.png — geospatial visualization

---

## Insights
complaint_trends.md
elevator_complaint_insights.md
