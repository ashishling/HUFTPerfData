# Data Request - Saras Performance Scorecard (Phase 1)

## Purpose
Build a reliable Saras scorecard split into:
- Brand-building performance
- Conversion performance

This will be used to review historical performance and guide budget reallocation decisions.

## Time Window
- Historical baseline: **Dec 1, 2025 - Feb 28, 2026**
- Ongoing tracking: **Mar 1, 2026 - Apr 30, 2026** (weekly refresh)

## Scope (Phase 1)
- Brand: **Saras only**
- Channels: Meta, Google, Shopify (D2C), Quick commerce (Blinkit/Instamart/Zepto)
- Geography: India
- Currency: INR

## File Naming Convention
Use this format for all uploads:
`<source>_<brand>_<grain>_<startdate>_<enddate>.csv`

Example:
`meta_saras_campaign_2025-12-01_2026-02-28.csv`

## Required Data Exports

### 1) Meta Ads (campaign/ad set level)
**Granularity:** Daily and Campaign (both, if available)

**Required columns**
- Date
- Campaign ID, Campaign name
- Ad set ID, Ad set name
- Attribution setting (critical)
- Spend (INR)
- Impressions
- Reach
- Frequency
- Link clicks
- CTR (link)
- CPC (link)
- Video views (if available)
- Video watched 50% (or ThruPlay + watch % fields available)
- Purchases / Results
- Purchase value / Conversion value
- Purchase ROAS
- Website purchase ROAS

**Notes**
- Please include campaigns tagged to Saras even if naming is inconsistent.
- Include any current campaign taxonomy/mapping sheet if available.

### 2) Google Ads (campaign level)
**Granularity:** Daily and Campaign

**Required columns**
- Date
- Campaign ID, Campaign name
- Campaign type
- Cost (INR)
- Impressions
- Clicks
- CTR
- Avg CPC
- Conversions
- Orders (if tracked)
- Conversion value / Revenue / Purchase value
- ROAS
- Video played to 50% and 100% (where relevant)

### 3) Shopify Attribution Reports (Meta + Google)
**Granularity:** Daily if possible, else monthly export with campaign split

**Required columns**
- Date (or report month)
- UTM source
- UTM medium
- UTM campaign
- Orders
- Gross sales
- Net sales
- Total sales
- Discounts
- Returns
- AOV
- New vs returning customer orders (if available)

### 4) Quick Commerce Performance (Blinkit / Instamart / Zepto)
**Granularity:** Weekly or daily (preferred)

**Required columns**
- Date/week
- Platform (Blinkit/Instamart/Zepto)
- Brand
- SKU/category (if available)
- Spend type (platform media / CPA / promo)
- Spend amount (INR)
- Orders
- GMV / Revenue
- CPA (if shared by platform)
- New customer count (if available)
- Repeat customer count (if available)
- Any attribution/lookback window details

### 5) Brand Search / Demand Signals

**Google branded search**
- Date (weekly or monthly)
- Query term
- Branded query flag (Saras terms)
- Impressions
- Clicks

**HUFT site search**
- Date
- Search term
- Search volume

**Quick commerce search (if available)**
- Platform
- Date/week
- Branded keyword/search volume

### 6) Budget + Planning Inputs

**Required columns**
- Month/week
- Brand (Saras)
- Channel (Meta/Google/QCom/etc.)
- Planned budget
- Actual spend
- Notes on objective (brand-building vs conversion)

### 7) Data Dictionary / Logic Notes (mandatory)
Please share a short note covering:
- Metric definitions used internally (ROAS, CPA, purchase, revenue)
- Attribution windows by platform
- Any GST adjustment applied in current reporting (e.g., x1.18)
- Known exclusions or manual adjustments in current sheets

## Submission Cadence

### One-time (immediate)
- Full historical dump for Dec 2025 - Feb 2026

### Weekly (ongoing till Apr 2026)
- Incremental refresh each Monday for previous week data

## Submission Format
- Preferred: CSV files in a single folder + one README note
- Alternate: Google Sheet tabs with identical schema and locked headers

## Owners and Timeline
- Data owners: Mirnalini / Gunjan / channel owners
- First complete dump requested by: **EOD Friday, March 6, 2026**
- Weekly refresh starting: **Monday, March 9, 2026**

## Acceptance Criteria
Data request is complete when:
- All files received for Saras for Dec-Feb
- Attribution setting is present for Meta exports
- QCom spend and order/revenue are available at least weekly
- UTM/campaign fields allow mapping between ad platforms and Shopify
