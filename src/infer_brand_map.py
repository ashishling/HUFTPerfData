from __future__ import annotations

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / 'data' / 'raw'
DERIVED = ROOT / 'data' / 'derived'
DERIVED.mkdir(parents=True, exist_ok=True)

META_FILE = RAW / 'Meta Report (Jan1-31).csv'
GOOGLE_FILE = RAW / 'Campaign report_Google_Jan 2026.csv'

# Brand keyword dictionary (expandable)
BRAND_KEYWORDS = {
    "Sara's": ["sara", "sara's", "saratreats", "sara treats"],
    "Treats Portfolio": ["treats", "treat"],
    "Sara's Treats": ["saratreats", "sara treats", "sara's treats"],
    "Yakies": ["yakies"],
    "YIMT": ["yimt"],
    "Yum Num": ["yumnum", "yum num"],
    "Hearty": ["hearty", "heartyk"],
    "Meowsi": ["meowsi"],
    "NutriMeow": ["nutrimeow", "nutri meow"],
    "HUFT Treats": ["huft treats", "hufttreats", "huft treat"],
}


def infer_mapping(name: str, platform: str) -> list[tuple[str, str, str]]:
    name_l = name.lower()
    matches = []
    for brand, keys in BRAND_KEYWORDS.items():
        for k in keys:
            if k in name_l:
                matches.append((brand, platform, f"%{k}%"))
                break
    return matches


rows = []

# Meta campaigns
if META_FILE.exists():
    meta = pd.read_csv(META_FILE)
    for name in meta['Campaign name'].dropna().unique():
        rows.extend(infer_mapping(name, 'meta'))

# Google campaigns
if GOOGLE_FILE.exists():
    google = pd.read_csv(GOOGLE_FILE, encoding='utf-16', sep='\t', skiprows=2)
    for name in google['Campaign'].dropna().unique():
        rows.extend(infer_mapping(name, 'google'))

# Deduplicate
rows = sorted(set(rows))

out = DERIVED / 'brand_map_inferred.csv'
# If no matches, still write header
if rows:
    df = pd.DataFrame(rows, columns=['brand', 'platform', 'pattern'])
    df.to_csv(out, index=False)
else:
    out.write_text('brand,platform,pattern\n')

print('Wrote', out)
