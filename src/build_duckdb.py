from __future__ import annotations

import re
from pathlib import Path
import pandas as pd
import duckdb

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / 'data' / 'raw'
DERIVED = ROOT / 'data' / 'derived'
DB_PATH = DERIVED / 'analytics.duckdb'
DASHBOARD_MARTS_DIR = DERIVED / 'dashboard_v2' / 'marts'
DASHBOARD_STAGING_DIR = DERIVED / 'dashboard_v2' / 'staging'

DERIVED.mkdir(parents=True, exist_ok=True)

NUM_CLEAN_RE = re.compile(r"[^0-9.\-]")


def clean_numeric_series(s: pd.Series) -> pd.Series:
    # Convert strings like '1,234.50', '3.2%', ' --' to float
    return (
        s.astype(str)
        .replace({'--': '', 'nan': ''})
        .str.replace(',', '', regex=False)
        .str.replace('%', '', regex=False)
        .replace({'': None, 'None': None})
        .apply(lambda v: None if v is None else NUM_CLEAN_RE.sub('', v))
        .astype(float)
    )


def coerce_numeric(df: pd.DataFrame, exclude: set[str]) -> pd.DataFrame:
    for col in df.columns:
        if col in exclude:
            continue
        # attempt to coerce object/string columns to numeric
        if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            try:
                df[col] = clean_numeric_series(df[col])
            except Exception:
                # leave as-is if conversion fails
                pass
    return df


# --- Google Ads Campaign report ---
GOOGLE_FILE = RAW / 'Campaign report_Google_Jan 2026.csv'
if GOOGLE_FILE.exists():
    google = pd.read_csv(GOOGLE_FILE, encoding='utf-16', sep='\t', skiprows=2)
    # Drop total rows
    google = google[~google['Campaign status'].astype(str).str.startswith('Total:', na=False)]
    # Drop zero/empty cost rows
    google['Cost'] = pd.to_numeric(google['Cost'].astype(str).str.replace(',', ''), errors='coerce')
    google = google[google['Cost'].notna() & (google['Cost'] > 0)]

    google_exclude = {
        'Campaign status', 'Campaign', 'Budget name', 'Budget type', 'Status',
        'Status reasons', 'Campaign type', 'Currency code', 'Bid strategy type'
    }
    google = coerce_numeric(google, google_exclude)
else:
    google = None


# --- Meta report ---
META_FILE = RAW / 'Meta Report (Jan1-31).csv'
if META_FILE.exists():
    meta = pd.read_csv(META_FILE)
    # Drop zero/empty spend rows
    meta['Amount spent (INR)'] = pd.to_numeric(meta['Amount spent (INR)'], errors='coerce')
    meta = meta[meta['Amount spent (INR)'].notna() & (meta['Amount spent (INR)'] > 0)]

    meta_exclude = {
        'Campaign name', 'Campaign delivery', 'Attribution setting', 'Result indicator', 'Ends'
    }
    meta = coerce_numeric(meta, meta_exclude)
else:
    meta = None


# --- Shopify Google report ---
SHOPIFY_GOOGLE_FILE = RAW / 'Google Ads_Shopify Report.csv'
if SHOPIFY_GOOGLE_FILE.exists():
    shop_g = pd.read_csv(SHOPIFY_GOOGLE_FILE)
    shop_g_exclude = {'Order UTM medium', 'Order UTM campaign'}
    shop_g = coerce_numeric(shop_g, shop_g_exclude)
else:
    shop_g = None


# --- Shopify Facebook report ---
SHOPIFY_FB_FILE = RAW / 'Shopify - Facebook Report 🌎.csv'
if SHOPIFY_FB_FILE.exists():
    shop_f = pd.read_csv(SHOPIFY_FB_FILE)
    shop_f_exclude = {'Order UTM campaign', 'Order UTM content'}
    shop_f = coerce_numeric(shop_f, shop_f_exclude)
else:
    shop_f = None

# --- Dashboard monthly marts (Dec 2025 / Jan 2026 / Feb 2026) ---
MART_FILES = {
    'mart_conversion_channel_monthly': DASHBOARD_MARTS_DIR / 'mart_conversion_channel_monthly.csv',
    'mart_exec_summary_monthly': DASHBOARD_MARTS_DIR / 'mart_exec_summary_monthly.csv',
    'mart_reconciliation_monthly': DASHBOARD_MARTS_DIR / 'mart_reconciliation_monthly.csv',
}

monthly_marts: dict[str, pd.DataFrame | None] = {}
for tname, path in MART_FILES.items():
    if not path.exists():
        monthly_marts[tname] = None
        continue
    df = pd.read_csv(path)
    monthly_exclude = {
        'month_start', 'brand', 'channel', 'sub_channel', 'objective_class',
        'attribution_layer', 'data_confidence', 'source_table', 'metric_name',
        'status', 'status_notes'
    }
    monthly_marts[tname] = coerce_numeric(df, monthly_exclude)


# --- Dashboard monthly staging campaign-level inputs ---
STAGING_FILES = {
    'google_campaigns_monthly': DASHBOARD_STAGING_DIR / 'google_ads_campaigns_monthly.csv',
    'meta_campaigns_monthly': DASHBOARD_STAGING_DIR / 'meta_d2c_adset_monthly.csv',
}

monthly_staging: dict[str, pd.DataFrame | None] = {}
for tname, path in STAGING_FILES.items():
    if not path.exists():
        monthly_staging[tname] = None
        continue
    df = pd.read_csv(path)
    if tname == 'google_campaigns_monthly' and 'Cost' in df.columns:
        df['Cost'] = pd.to_numeric(df['Cost'], errors='coerce')
        df = df[df['Cost'].notna() & (df['Cost'] > 0)]
        exclude = {
            'Campaign status', 'Campaign', 'Budget name', 'Budget type', 'Status',
            'Status reasons', 'Campaign type', 'Currency code', 'Bid strategy type',
            'month_start', 'source_file'
        }
        monthly_staging[tname] = coerce_numeric(df, exclude)
    elif tname == 'meta_campaigns_monthly' and 'Amount spent (INR)' in df.columns:
        df['Amount spent (INR)'] = pd.to_numeric(df['Amount spent (INR)'], errors='coerce')
        df = df[df['Amount spent (INR)'].notna() & (df['Amount spent (INR)'] > 0)]
        exclude = {
            'Ad set name', 'Attribution setting', 'Result value indicator',
            'month_start', 'source_file'
        }
        monthly_staging[tname] = coerce_numeric(df, exclude)
    else:
        monthly_staging[tname] = df


con = duckdb.connect(DB_PATH)

if google is not None:
    con.execute('DROP TABLE IF EXISTS google_campaigns')
    con.execute('CREATE TABLE google_campaigns AS SELECT * FROM google')

if meta is not None:
    con.execute('DROP TABLE IF EXISTS meta_campaigns')
    con.execute('CREATE TABLE meta_campaigns AS SELECT * FROM meta')

if shop_g is not None:
    con.execute('DROP TABLE IF EXISTS shopify_google')
    con.execute('CREATE TABLE shopify_google AS SELECT * FROM shop_g')

if shop_f is not None:
    con.execute('DROP TABLE IF EXISTS shopify_facebook')
    con.execute('CREATE TABLE shopify_facebook AS SELECT * FROM shop_f')

for tname, df in monthly_marts.items():
    if df is None:
        continue
    con.execute(f'DROP TABLE IF EXISTS {tname}')
    con.execute(f'CREATE TABLE {tname} AS SELECT * FROM df')

for tname, df in monthly_staging.items():
    if df is None:
        continue
    con.execute(f'DROP TABLE IF EXISTS {tname}')
    con.execute(f'CREATE TABLE {tname} AS SELECT * FROM df')

if monthly_marts.get('mart_conversion_channel_monthly') is not None:
    con.execute('DROP VIEW IF EXISTS monthly_channel_spend')
    con.execute(
        '''
        CREATE VIEW monthly_channel_spend AS
        SELECT
          month_start,
          channel,
          SUM(spend) AS spend_inr
        FROM mart_conversion_channel_monthly
        GROUP BY 1,2
        ORDER BY 1,2
        '''
    )

    con.execute('DROP VIEW IF EXISTS monthly_paid_spend_meta_google')
    con.execute(
        '''
        CREATE VIEW monthly_paid_spend_meta_google AS
        SELECT
          month_start,
          SUM(CASE WHEN LOWER(channel) = 'meta' THEN spend ELSE 0 END) AS meta_spend_inr,
          SUM(CASE WHEN LOWER(channel) = 'google' THEN spend ELSE 0 END) AS google_spend_inr,
          SUM(CASE WHEN LOWER(channel) IN ('meta', 'google') THEN spend ELSE 0 END) AS total_spend_inr
        FROM mart_conversion_channel_monthly
        GROUP BY 1
        ORDER BY 1
        '''
    )

con.close()

print('DuckDB created at', DB_PATH)
print('Tables:')
for tname in ['google_campaigns','meta_campaigns','shopify_google','shopify_facebook']:
    if (google is not None and tname=='google_campaigns') or \
       (meta is not None and tname=='meta_campaigns') or \
       (shop_g is not None and tname=='shopify_google') or \
       (shop_f is not None and tname=='shopify_facebook'):
        print('-', tname)

for tname, df in monthly_marts.items():
    if df is not None:
        print('-', tname)

for tname, df in monthly_staging.items():
    if df is not None:
        print('-', tname)
