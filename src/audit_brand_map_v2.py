from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
BRAND_MAP_PATH = ROOT / "data" / "derived" / "brand_map.csv"
STAGING_DIR = ROOT / "data" / "derived" / "dashboard_v2" / "staging"
OUT_DIR = ROOT / "data" / "derived" / "dashboard_v2" / "audits"


def like_to_regex(pattern: str) -> str:
    escaped = re.escape(str(pattern).lower()).replace("%", ".*")
    return f"^{escaped}$"


def build_mapper(brand_map: pd.DataFrame):
    bm = brand_map.copy()
    bm["regex"] = bm["pattern"].astype(str).apply(like_to_regex)

    def map_brand(text: str, platform: str) -> str | None:
        t = str(text).lower()
        sub = bm[bm["platform"].str.lower() == platform.lower()]
        hits = []
        for _, r in sub.iterrows():
            if re.search(r["regex"], t):
                hits.append((r["brand"], int(r["priority"]), len(str(r["pattern"]))))
        if not hits:
            return None
        hits.sort(key=lambda x: (x[1], x[2]), reverse=True)
        return hits[0][0]

    return map_brand


def collect_entities(map_brand_fn) -> pd.DataFrame:
    rows: list[dict[str, str]] = []

    # Google campaigns
    p = STAGING_DIR / "google_ads_campaigns_monthly.csv"
    if p.exists():
        g = pd.read_csv(p, usecols=["Campaign", "month_start"]).dropna(subset=["Campaign"])
        g = g.drop_duplicates()
        for _, r in g.iterrows():
            rows.append(
                {
                    "source": "google_campaign",
                    "platform": "google",
                    "month_start": str(r["month_start"]),
                    "entity_name": str(r["Campaign"]),
                    "mapped_brand": map_brand_fn(r["Campaign"], "google"),
                }
            )

    # Meta D2C adsets
    p = STAGING_DIR / "meta_d2c_adset_monthly.csv"
    if p.exists():
        m = pd.read_csv(p, usecols=["Ad set name", "month_start"]).dropna(subset=["Ad set name"])
        m = m.drop_duplicates()
        for _, r in m.iterrows():
            rows.append(
                {
                    "source": "meta_d2c_adset",
                    "platform": "meta",
                    "month_start": str(r["month_start"]),
                    "entity_name": str(r["Ad set name"]),
                    "mapped_brand": map_brand_fn(r["Ad set name"], "meta"),
                }
            )

    # Blinkit/Instamart Meta adsets
    for fname, src in [
        ("meta_blinkit_monthly.csv", "meta_blinkit"),
        ("meta_instamart_monthly.csv", "meta_instamart"),
    ]:
        p = STAGING_DIR / fname
        if not p.exists():
            continue
        d = pd.read_csv(p)
        if "Ad set name" not in d.columns:
            continue
        d = d.dropna(subset=["Ad set name"]).drop_duplicates(subset=["Ad set name", "month_start"])
        for _, r in d.iterrows():
            rows.append(
                {
                    "source": src,
                    "platform": "meta",
                    "month_start": str(r.get("month_start", "")),
                    "entity_name": str(r["Ad set name"]),
                    "mapped_brand": map_brand_fn(r["Ad set name"], "meta"),
                }
            )

    return pd.DataFrame(rows)


def classify_confidence(entity_name: str) -> str:
    t = str(entity_name).lower()
    high_tokens = [
        " swf",
        "swf_",
        "_swf",
        "sara's",
        "saras",
        "wholesome",
    ]
    if any(tok in t for tok in high_tokens):
        return "high"
    if "dog food" in t:
        return "low"
    return "medium"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    brand_map = pd.read_csv(BRAND_MAP_PATH)
    map_brand_fn = build_mapper(brand_map)
    all_entities = collect_entities(map_brand_fn)

    if all_entities.empty:
        print("No entities found to audit.")
        return

    all_entities["confidence"] = all_entities["entity_name"].apply(classify_confidence)

    # Candidates where Saras-signal exists but mapped brand is not Sara's.
    saras_signal_re = re.compile(r"(sara|swf|wholesome)", re.I)
    all_entities["saras_signal"] = all_entities["entity_name"].astype(str).apply(
        lambda x: bool(saras_signal_re.search(x))
    )

    candidates = all_entities[
        all_entities["saras_signal"] & (all_entities["mapped_brand"].fillna("") != "Sara's")
    ].copy()

    high = candidates[candidates["confidence"] == "high"].copy()
    medium_low = candidates[candidates["confidence"] != "high"].copy()

    summary = (
        candidates.groupby(["source", "mapped_brand", "confidence"], dropna=False)
        .size()
        .reset_index(name="entities")
        .sort_values(["confidence", "entities"], ascending=[True, False])
    )

    all_entities.to_csv(OUT_DIR / "brand_mapping_audit_all_entities.csv", index=False)
    candidates.to_csv(OUT_DIR / "brand_mapping_audit_saras_candidates_not_mapped.csv", index=False)
    high.to_csv(OUT_DIR / "brand_mapping_audit_saras_high_confidence_not_mapped.csv", index=False)
    medium_low.to_csv(
        OUT_DIR / "brand_mapping_audit_saras_medium_low_confidence_not_mapped.csv", index=False
    )
    summary.to_csv(OUT_DIR / "brand_mapping_audit_summary.csv", index=False)

    print("All entities:", len(all_entities))
    print("Saras-signal not mapped to Sara's:", len(candidates))
    print("High-confidence Saras-signal not mapped:", len(high))
    print("\nTop high-confidence examples:")
    print(
        high[["source", "month_start", "entity_name", "mapped_brand"]]
        .head(20)
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()
