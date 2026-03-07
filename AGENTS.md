# Codex Context

## Project Goal
Enable fast, reliable access to Meta + Google Ads + Shopify data for analysis and LLM-driven reporting.

## Current State
- Source files moved to `data/raw/`.
- Local DuckDB database built at `data/derived/analytics.duckdb`.
- Cleaning: removed total rows and zero‑spend rows in Google and Meta reports.
- Brand mapping file created at `data/derived/brand_map.csv` (priority‑based matching).
- Views created:
  - `meta_spend_by_brand`
  - `google_spend_by_brand`
  - `spend_by_brand`

## Key Scripts
- `src/build_duckdb.py` – builds DuckDB from raw CSVs

## Important Constraints
- Data is aggregate monthly CSVs (no daily breakdown yet).
- PDF files are reference reports (Jan 1–27, 2026).
- Brand bucketing requires curated mapping patterns.

## Common Queries
```sql
SELECT * FROM spend_by_brand;
```

## Archived Files
Intermediate CSVs moved to `data/archive/`.
