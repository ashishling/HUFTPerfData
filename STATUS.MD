# Dashboard Build Status

Last updated: 2026-03-06 (post dashboard enhancements)

## Git Remote
- Repository remote: `https://github.com/ashishling/HUFTPerfData.git`
- Notes:
  - Local workspace was initialized as a git repository and linked to this remote.
  - Push target is this remote for ongoing HUFT mart + MCP updates.

## How To Refresh
Use this when new source files are added/updated in `dashboard_data/`.

1. Rebuild staging + manifest + validation
   - `python3 src/etl_dashboard_v2.py`
2. Rebuild curated marts
   - `python3 src/build_dashboard_marts_v2.py`
3. Rebuild dashboard package
   - `python3 src/build_dashboard_v1.py`
4. Optional: rerun brand mapping audit
   - `python3 src/audit_brand_map_v2.py`

Primary refreshed outputs:
- `data/derived/dashboard_v2/staging/`
- `data/derived/dashboard_v2/marts/`
- `data/derived/dashboard_v2/dashboard_v1/dashboard_v1.html`

## Architecture
```text
dashboard_data/ (raw files: csv/xlsx)
        |
        v
src/etl_dashboard_v2.py
  - file discovery + classification
  - month parsing
  - manifest + coverage + awaited flags
  - staging table generation
  - brand-map guardrails
        |
        +--> data/derived/dashboard_v2/data_manifest.csv
        +--> data/derived/dashboard_v2/month_coverage.csv
        +--> data/derived/dashboard_v2/validation/brand_map_guardrails.csv
        +--> data/derived/dashboard_v2/staging/*.csv
        |
        v
src/build_dashboard_marts_v2.py
  - apply brand mapping to Saras scope
  - derive conversion / demand / reconciliation marts
  - data quality issue table
        |
        +--> data/derived/dashboard_v2/marts/*.csv
        |
        v
src/build_dashboard_v1.py
  - reshape marts into dashboard tabs
  - generate interactive HTML (sort/filter/select/download)
  - export tab csvs
        |
        +--> data/derived/dashboard_v2/dashboard_v1/dashboard_v1.html
        +--> data/derived/dashboard_v2/dashboard_v1/tab*.csv

Optional diagnostics:
src/audit_brand_map_v2.py
  -> data/derived/dashboard_v2/audits/*.csv
```

### Key Components
- Raw layer: `dashboard_data/`
- Staging layer: `data/derived/dashboard_v2/staging/`
- Curated mart layer: `data/derived/dashboard_v2/marts/`
- Presentation layer: `data/derived/dashboard_v2/dashboard_v1/`
- Mapping/config layer: `data/derived/brand_map.csv`
- Ops/traceability layer:
  - `data_manifest.csv`
  - `month_coverage.csv`
  - `STATUS.MD`

## Project Summary
- Project: Saras performance dashboard V1 from raw monthly extracts (Dec 2025 to Feb 2026).
- Objective: enable decision-ready review of spend, attributed revenue, ROAS, reconciliation, and campaign-brand mapping.
- Current state: functional interactive HTML dashboard with sortable/filterable tables, per-table CSV download, and row-selection sums.

## Data and Pipeline Architecture
- Raw source drop: `dashboard_data/`
- ETL/staging:
  - script: `src/etl_dashboard_v2.py`
  - outputs: `data/derived/dashboard_v2/staging/`
  - manifest + coverage:
    - `data/derived/dashboard_v2/data_manifest.csv`
    - `data/derived/dashboard_v2/month_coverage.csv`
  - validation:
    - `data/derived/dashboard_v2/validation/brand_map_guardrails.csv`
- Curated marts:
  - script: `src/build_dashboard_marts_v2.py`
  - outputs: `data/derived/dashboard_v2/marts/`
- Dashboard package:
  - script: `src/build_dashboard_v1.py`
  - outputs: `data/derived/dashboard_v2/dashboard_v1/`

## Core Decisions Applied
- Brand mapping is driven by `data/derived/brand_map.csv`.
- Mapping rules locked:
  - `treat/treats` campaigns should not map to `Sara's`.
  - `wholesome` campaigns should map to `Sara's`.
- Guardrails enforced in ETL:
  - `wholesome -> Sara's` required
  - `treat/treats -> not Sara's` required
- Dashboard channel naming (presentation layer):
  - `blinkit` -> `blinkit_cpass`
  - `instamart` -> `instamart_cpass`
- Jan file correction accepted:
  - `SWF-ShopifyDashboardXMeta-Jan_25.csv` renamed and treated as Jan 2026.
- Blinkit Jan c-pass issue fixed by manually adding missing `Ad set name` in source file:
  - `BlinkItXHuft_Saras_Adsetmissing`

## Completed
- Rewrote dashboard structure to P0-first format in `dashboardstructutre.md`.
- Added ETL script: `src/etl_dashboard_v2.py`.
- Generated manifest and month coverage under `data/derived/dashboard_v2/`.
- Added status/action tracking inside `data_manifest.csv`:
  - `status`
  - `action_required`
- Added curated mart builder: `src/build_dashboard_marts_v2.py`.
- Added brand-map audit script: `src/audit_brand_map_v2.py`.
- Applied `brand_map.csv` filtering in mart build for Google/Meta campaign-level sources.
- Added ETL brand-map guardrails:
  - `wholesome` must map to `Sara's`
  - `treat/treats` must not map to `Sara's`
- Built dashboard V1 package from available marts:
  - `src/build_dashboard_v1.py`
  - output: `data/derived/dashboard_v2/dashboard_v1/dashboard_v1.html`
- Added Tab 2 campaign-level double-click tables:
  - Google campaign-level conversion insights
  - Meta campaign/ad set-level conversion insights
- Added Google-Shopify campaign key normalization for Tab 2 join and explicit alias mapping:
  - `Pmax_Sara's_New_Customer` -> `Pmax_Sara's_wholesome_New_Customer`
- Added new demand generation metrics:
  - `Meta_New` (month-wise from `meta_shopify_sales_monthly.csv` -> `New customers`)
  - `D2C_New` and `D2C_Repeat` (month-wise from `shopify_new_vs_returning_daily.csv`)
- Exported additional Tab 2 files:
  - `tab2_google_campaign_insights.csv`
  - `tab2_meta_campaign_insights.csv`

## Current Outputs
- Staging files: `data/derived/dashboard_v2/staging/`
- Mart files: `data/derived/dashboard_v2/marts/`
  - `mart_exec_summary_monthly.csv`
  - `mart_conversion_channel_monthly.csv`
  - `mart_demand_capture_monthly.csv`
  - `mart_demand_generation_monthly.csv`
  - `mart_reconciliation_monthly.csv`
  - `mart_data_quality_issues.csv`
- Dashboard V1 package: `data/derived/dashboard_v2/dashboard_v1/`
  - `dashboard_v1.html`
  - tab-level CSV exports (`tab1` to `tab6`)
- One-time analysis outputs: `data/derived/dashboard_v2/analysis/`
  - `one_time_iroas_spend_cut_analysis.csv`

## Latest Build Run
- `python3 src/build_dashboard_marts_v2.py` executed successfully.
- `python3 src/build_dashboard_v1.py` executed successfully.
- Current mart row counts:
  - `mart_conversion_channel_monthly.csv`: 26
  - `mart_demand_capture_monthly.csv`: 20
  - `mart_demand_generation_monthly.csv`: 21
  - `mart_reconciliation_monthly.csv`: 6
  - `mart_exec_summary_monthly.csv`: 3
  - `mart_data_quality_issues.csv`: 3

## Brand Mapping Audit (Dec-Feb)
- Audit outputs generated in `data/derived/dashboard_v2/audits/`.
- High-confidence Saras-signal campaigns not mapped to `Sara's`: 18 rows.
- Main pattern observed:
  - Campaigns containing both `sara/saras` and `treat/treats` are mapped to `Treats Portfolio` (higher priority rule 200).
  - `...Wholesome...` now maps to `Sara's` after adding `wholesome` patterns.
- Key file:
  - `brand_mapping_audit_saras_high_confidence_not_mapped.csv`
 - Mapping decision applied:
   - Any campaign with `treat/treats` should not map to `Sara's`.
   - Any campaign with `wholesome` should map to `Sara's`.

## Pending Data (Awaited)
- `meta_instamart` for `2025-12`.
- `saras_total_revenue_by_channel` for Dec 2025 to Feb 2026 (D2C, Offline, Q-Com, etc.).

## Open Issues / Risks
- Multiple sources are `.xlsx` and not yet in automated parse path:
  - Amazon monthly sales
  - Platform-wise spend
  - Blinkit indexed file
  - Instamart indexed file
- `brand_search_google_raw.csv` has missing Feb 2026 branded search value (blank in current table).
- MER is blank because total revenue all channels file is pending.
- Metric logic caveat: filtering now uses `brand_map.csv`; final accuracy depends on rule quality and priority settings.
- `brand_map.csv` rule priority may be causing cross-tagging for Saras Treats vs Saras Wholesome intent.
- Google vs Shopify campaign attribution matching still has residual naming drift beyond current explicit alias rules; some Shopify campaign revenue may remain unmatched at campaign level without a maintained crosswalk.

## Dashboard V1 Feature Set
- Interactive tables:
  - Column sorting
  - Per-column filtering
  - Row selection with live numeric sums
  - Per-table "Download CSV" export
- Tabs:
  1. Executive Summary
  2. Conversion Performance:
     - Channel-level leadership view
     - Google campaign-level double-click
     - Meta campaign/ad set-level double-click
  3. Demand Capture (split paid vs shopify attribution revenue + ROAS)
  4. Demand Generation (month-wise):
     - `meta_spend`, `meta_impressions`, `meta_reach`
     - `Meta_New`
     - `google_branded_search_volume`
     - `D2C_New`, `D2C_Repeat`
  5. Reconciliation and Data Quality
  6. Campaign to Brand Mapping (with attribution setting where available; non-zero spend only)

## Next Actions
1. Receive pending files listed above.
2. Re-run pipeline when new files arrive:
   - `python3 src/etl_dashboard_v2.py`
   - `python3 src/build_dashboard_marts_v2.py`
   - `python3 src/audit_brand_map_v2.py`
3. Rebuild dashboard package:
   - `python3 src/build_dashboard_v1.py`
4. Validate refreshed marts and reconciliation deltas.
5. Ingest pending total revenue by channel file and populate `MER` in `mart_exec_summary_monthly.csv`.
6. Upgrade V1 HTML package to tool-facing dashboard (Sheets/BI) on the same marts.
