# Dashboard Structure V2 (P0 Only)

## Objective
Create a decision-ready performance dashboard for Saras that supports:
- Channel budget allocation decisions (monthly and weekly)
- Clear split between demand capture and demand generation
- Reconciliation of attribution differences before actioning insights

## Time Horizon and Grain
- Historical baseline: Dec 2025 to Feb 2026
- Ongoing tracking: Mar 2026 onward (weekly refresh)
- Reporting grain:
  - Weekly for operational decisions
  - Monthly for management review

## Metric Layers (Mandatory)
All tabs must separate and label metrics into these layers:
- Platform-attributed metrics (Meta/Google/QCom platform reporting)
- Shopify-attributed metrics (last-click/UTM based)
- Blended metrics (MER and total business outcome view)

## Dashboard Tabs (Decision-First)

### Tab 1 - Executive Summary (Brand-Level)
Purpose: fast monthly/weekly decision view for Saras.

Required metrics:
- Total revenue
- Paid spend
- Paid attributed revenue (platform and Shopify shown separately)
- Paid ROAS (platform and Shopify shown separately)
- MER (Total Revenue / Paid Spend)
- Channel spend mix (% by channel)

Required dimensions:
- Week/Month
- Brand
- Channel
- Attribution layer

### Tab 2 - Conversion Performance
Purpose: compare conversion efficiency across demand capture channels and D2C paid.

Required metrics:
- Spend
- Orders
- Revenue
- CPA (where available)
- ROAS (platform and Shopify variants)

Required dimensions:
- Week/Month
- Channel (Meta, Google, Blinkit, Instamart, Zepto, etc.)
- Sub-channel/type (e.g., Search, CPA, Platform Ads)
- Attribution layer

### Tab 3 - Demand Capture
Purpose: evaluate channels where intent already exists.

Scope:
- Google Search
- Q-Commerce paid capture activity

Required metrics:
- Spend
- Orders
- Revenue
- CPA
- ROAS

Required dimensions:
- Week/Month
- Channel/sub-channel
- Attribution layer

### Tab 4 - Demand Generation
Purpose: evaluate upper/mid-funnel investment quality.

Scope:
- Meta (and other channels classified as demand generation)

Required metrics:
- Spend
- Reach
- Impressions
- 50% video views (or closest available stable engagement metric)
- Branded search trend indicators (Google / HUFT / QCom where available)

Required dimensions:
- Week/Month
- Channel
- Campaign objective

### Tab 5 - Reconciliation and Data Quality (Mandatory)
Purpose: prevent wrong decisions from attribution or data inconsistencies.

Required views:
- Meta vs Shopify attributed revenue and ROAS delta
- Channel-level attribution delta by month/week
- Missing data tracker (by source and period)
- Unmapped campaign tracker

## Campaign Objective Split (Mandatory)
Every campaign/spend line should be classified into one objective:
- `demand_capture`
- `demand_generation`

This mapping should be maintained in a separate mapping table and versioned.

## Data Model Keys (Mandatory)
All transformed tables feeding the dashboard must include:
- `date` (or week_start/month)
- `brand`
- `channel`
- `sub_channel` (if applicable)
- `campaign_name` (if available)
- `campaign_id` (if available)
- `objective_class` (demand_capture / demand_generation)
- `attribution_layer` (platform / shopify / blended)

## Data Contracts by Source (P0 Requirement)
For each source, define and enforce:
- Owner
- File name convention
- Required columns
- Refresh cadence
- Coverage window

Minimum sources in V2:
- Meta Ads export
- Google Ads export
- Shopify attribution exports (Meta + Google)
- Q-Commerce performance exports
- Brand search exports (Google/HUFT/QCom where available)

## ETL Flow (P0)
Dashboard should not read raw CSVs directly.

Pipeline:
1. Raw layer: ingest CSV extracts as-is.
2. Staging layer: standardize schema, types, and naming.
3. Curated layer: build dashboard-ready weekly/monthly aggregates.
4. Validation: run completeness and consistency checks before refresh.

Minimum validation checks:
- Missing period/source check
- Duplicate row check on key fields
- Null check on required keys
- Spend/revenue sanity bounds
- Attribution layer completeness
