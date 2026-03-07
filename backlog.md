# Backlog

## Global Channel Filter (Dashboard V1)
- Add a global filter to view data by selected channels: `meta`, `google`, `blinkit`, `instamart` (default: `All`).

### Scope
- Filter should affect spends and attributed revenue views across tabs.
- Primary use case: analyze one or more channels without manually filtering each table.

### Suggested Implementation
1. Add top-level multi-select control in `dashboard_v1.html`.
2. Ensure each tab dataset includes a normalized `channel` field.
3. Recompute Tab 1 summary metrics from filtered channel rows:
   - `paid_spend`
   - `paid_revenue_paid_attribution`
   - `paid_revenue_shopify_attribution`
   - ROAS metrics
4. Apply row-level filtering to Tabs 2–6 based on selected channels.
5. Add a `Reset filters` action.
6. Persist filter state in URL query params for shareable views.

### Notes
- Prefer filtering from in-memory parsed data, not only visible DOM rows.
- Keep compatibility with existing column filters/sorting and row-selection totals.
