# HUFT Marketing Data Workspace

This repo consolidates Meta + Google Ads CSVs and Shopify exports into a local DuckDB database for fast analytics and LLM-assisted querying.

## Current Structure
- `data/raw/` – source files (CSV + PDF)
- `data/derived/` – processed outputs and database
- `data/archive/` – intermediate or superseded CSVs
- `src/` – scripts

## Database
- DuckDB file: `data/derived/analytics.duckdb`
- Build script: `src/build_duckdb.py`

Tables created:
- `google_campaigns` (from Google Ads campaign CSV)
- `meta_campaigns` (from Meta Ads CSV)
- `shopify_google` (from Shopify Google UTM CSV)
- `shopify_facebook` (from Shopify Facebook UTM CSV)
- `mart_conversion_channel_monthly` (dashboard monthly mart)
- `mart_exec_summary_monthly` (dashboard monthly mart)
- `mart_reconciliation_monthly` (dashboard monthly mart)
- `google_campaigns_monthly` (dashboard monthly staging)
- `meta_campaigns_monthly` (dashboard monthly staging)

Views created:
- `monthly_channel_spend`
- `monthly_paid_spend_meta_google`

Cleaning rules applied during build:
- Google: drop `Total:` rows; drop `Cost <= 0`; coerce numerics
- Meta: drop `Amount spent (INR) <= 0`; coerce numerics
- Shopify: coerce numerics

## Brand Mapping
- Mapping file: `data/derived/brand_map.csv`
- Used to bucket campaigns into brands with priority-based matching.
- Views created in DuckDB:
  - `meta_spend_by_brand`
  - `google_spend_by_brand`
  - `spend_by_brand`

## Example Query
```sql
SELECT * FROM spend_by_brand;
```

## Notes
- CSVs are Jan 1–31, 2026 aggregates (no daily granularity).
- PDFs contain creative/performance summaries (Jan 1–27, 2026).

## MCP Access (Claude Desktop)
- MCP server script: `src/mcp_duckdb_server.py`
- Database: `data/derived/analytics.duckdb` (opened read-only by the MCP server)

### 1) Rebuild mart (optional, before querying)
```bash
./.venv/bin/python src/build_duckdb.py
```

### 2) Claude Desktop config
Edit `~/Library/Application Support/Claude/claude_desktop_config.json` and add:

```json
{
  "mcpServers": {
    "huft-duckdb": {
      "command": "/Users/ashishlingamneni/VI Local/HUFT/.venv/bin/python",
      "args": [
        "/Users/ashishlingamneni/VI Local/HUFT/src/mcp_duckdb_server.py"
      ]
    }
  }
}
```

If you already have other servers configured, merge just the `huft-duckdb` entry.

### 3) Restart Claude Desktop
After restart, ask Claude to call tools such as:
- `list_datasets`
- `describe_dataset` with `spend_by_brand`
- `query_readonly` with SQL like `SELECT * FROM spend_by_brand ORDER BY total_spend_inr DESC`
- `ask_mart` with natural language like:
  - `Can you tell me total spends for Jan?`
  - `Can you check spends for Feb 2026?`
  - `Top 5 Google campaigns by spend`
  - `Top 5 Google campaigns in Feb 2026`
  - `Show brand spend`

### Guardrails built into this server
- Read-only DuckDB connection
- Single-statement SQL only
- `SELECT`/`WITH` only
- Hard row limit of 5000
- `ask_mart` generates SQL only for common intents (total spend, top campaigns, brand spend)
- Month-aware intents currently support Dec 2025 / Jan 2026 / Feb 2026

## Next Steps
- Add a query runner (`src/query.py`) for ad‑hoc SQL.
- Extend brand mapping rules as needed.
- Ingest daily exports for Jan 1–27 alignment.
