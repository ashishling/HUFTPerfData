# Data Schema

This repo uses a local DuckDB database created by `src/build_duckdb.py`.

- Database path: `data/derived/analytics.duckdb`
- Source files live in: `data/raw/`

## Tables

### `google_campaigns`
Source: `data/raw/Campaign report_Google_Jan 2026.csv`

Key columns:
- `Campaign` (string)
- `Campaign status` (string)
- `Campaign type` (string)
- `Currency code` (string)
- `Cost` (float)
- `Impr.` (float)
- `Clicks` (float)
- `CTR` (float)
- `Avg. CPC` (float)
- `Orders` (float)
- `Purchase value` (float)
- `Revenue` (float)
- `Conv. value` (float)
- `ROAS` (float)

Notes:
- Rows with `Campaign status` starting with `Total:` are removed.
- Rows with `Cost <= 0` are removed.
- Numeric-like columns are coerced to floats where possible.

### `meta_campaigns`
Source: `data/raw/Meta Report (Jan1-31).csv`

Key columns:
- `Campaign name` (string)
- `Campaign delivery` (string)
- `Reporting starts` (date string)
- `Reporting ends` (date string)
- `Amount spent (INR)` (float)
- `Impressions` (float)
- `Reach` (float)
- `Frequency` (float)
- `Link clicks` (float)
- `CTR (link click-through rate)` (float)
- `CPC (cost per link click) (INR)` (float)
- `Purchase ROAS (return on ad spend)` (float)

Notes:
- Rows with `Amount spent (INR) <= 0` are removed.
- Numeric-like columns are coerced to floats where possible.

### `shopify_google`
Source: `data/raw/Google Ads_Shopify Report.csv`

Key columns:
- `Order UTM medium` (string)
- `Order UTM campaign` (string)
- `Orders` (float)
- `Total sales` (float)
- `Gross sales` (float)
- `Net sales` (float)
- `Average order value` (float)
- `Orders (previous_month)` (float)
- `Total sales (previous_month)` (float)
- `Gross sales (previous_month)` (float)
- `Net sales (previous_month)` (float)
- `Average order value (previous_month)` (float)

Notes:
- Numeric-like columns are coerced to floats where possible.

### `shopify_facebook`
Source: `data/raw/Shopify - Facebook Report 🌎.csv`

Key columns:
- `Order UTM campaign` (string)
- `Order UTM content` (string)
- `Orders` (float)
- `Gross sales` (float)
- `Net sales` (float)
- `Total sales` (float)
- `Discounts` (float)
- `Returns` (float)

Notes:
- Numeric-like columns are coerced to floats where possible.

## Build Script

- Run: `python src/build_duckdb.py`
- Output: `data/derived/analytics.duckdb`

## Example Queries

```sql
-- Top Meta spenders
SELECT "Campaign name", "Amount spent (INR)"
FROM meta_campaigns
ORDER BY "Amount spent (INR)" DESC
LIMIT 10;

-- Google campaign spend and orders
SELECT "Campaign", "Cost", "Orders", "Revenue"
FROM google_campaigns
ORDER BY "Cost" DESC;

-- Shopify Google sales by campaign
SELECT "Order UTM campaign", "Orders", "Total sales"
FROM shopify_google
ORDER BY "Total sales" DESC;
```

## Brand Mapping

Brand mapping lives in `data/derived/brand_map.csv` with columns:
- `brand`
- `platform` (`meta` or `google`)
- `pattern` (SQL LIKE pattern, lowercase matching recommended)
- `priority` (higher wins if multiple patterns match)

Use `priority` to avoid double-counting where multiple patterns match a campaign name.

Example (Meta spend by brand with priority):

```sql
WITH matches AS (
  SELECT
    m.*,
    b.brand,
    b.priority,
    ROW_NUMBER() OVER (
      PARTITION BY m."Campaign name"
      ORDER BY b.priority DESC, LENGTH(b.pattern) DESC
    ) AS rn
  FROM meta_campaigns m
  JOIN read_csv_auto('data/derived/brand_map.csv') b
    ON b.platform = 'meta'
   AND LOWER(m."Campaign name") LIKE b.pattern
)
SELECT
  brand,
  SUM("Amount spent (INR)") AS meta_spend_inr
FROM matches
WHERE rn = 1
GROUP BY brand
ORDER BY meta_spend_inr DESC;
```

## Data Dictionary
### google_campaigns columns
- `Campaign status`
- `Campaign`
- `Budget`
- `Budget name`
- `Budget type`
- `Status`
- `Status reasons`
- `Optimization score`
- `Campaign type`
- `Currency code`
- `TrueView avg. CPV`
- `Avg. CPM`
- `Interactions`
- `Interaction rate`
- `Avg. cost`
- `Cost`
- `Conv. (Platform Comparable)`
- `Cost / Conv. (Platform Comparable)`
- `Conv. value / Cost (Platform Comparable)`
- `Revenue`
- `CTR`
- `Avg. CPC`
- `Campaign ID`
- `All conv.`
- `Unique users`
- `Engagements`
- `Target ROAS`
- `TrueView views`
- `TrueView view rate (In-stream)`
- `TrueView view rate (In-feed)`
- `TrueView view rate (Shorts)`
- `Video played to 25%`
- `Video played to 50%`
- `Video played to 75%`
- `Video played to 100%`
- `ROAS`
- `Impr.`
- `Clicks`
- `Earned views`
- `Viewable rate`
- `Orders`
- `Purchase value`
- `Conv. rate`
- `Bid strategy type`
- `Viewable CTR`
- `Avg. viewable CPM`
- `Viewable impr.`
- `Participated in-app actions`
- `Conv. value`
- `Conv. value / cost`
- `Conversions`
- `Cost / Participated in-app action`
- `Cost / conv.`
- `Original conv. value`

### meta_campaigns columns
- `Reporting starts`
- `Reporting ends`
- `Campaign name`
- `Campaign delivery`
- `Attribution setting`
- `Results`
- `Result indicator`
- `Reach`
- `Frequency`
- `Cost per results`
- `Ad set budget`
- `Ad set budget type`
- `Amount spent (INR)`
- `Ends`
- `Impressions`
- `CPM (cost per 1,000 impressions) (INR)`
- `Link clicks`
- `CPC (cost per link click) (INR)`
- `CTR (link click-through rate)`
- `CTR (all)`
- `CPC (all) (INR)`
- `Clicks (all)`
- `Purchase ROAS (return on ad spend)`
- `Website purchase ROAS (return on advertising spend)`
- `In-app purchase ROAS (return on ad spend)`

### shopify_google columns
- `Order UTM medium`
- `Order UTM campaign`
- `Orders`
- `Total sales`
- `Gross sales`
- `Net sales`
- `Average order value`
- `Orders (previous_month)`
- `Total sales (previous_month)`
- `Gross sales (previous_month)`
- `Net sales (previous_month)`
- `Average order value (previous_month)`
- `Orders (previous_month) `
- `Total sales (previous_month) `
- `Gross sales (previous_month) `
- `Net sales (previous_month) `
- `Average order value (previous_month) `

### shopify_facebook columns
- `Order UTM campaign`
- `Order UTM content`
- `Orders`
- `Gross sales`
- `Discounts`
- `Returns`
- `Net sales`
- `Shipping charges`
- `Duties`
- `Additional fees`
- `Taxes`
- `Total sales`
