from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_DATA = ROOT / "dashboard_data"
OUT_DIR = ROOT / "data" / "derived" / "dashboard_v2"
STAGING_DIR = OUT_DIR / "staging"
MANIFEST_PATH = OUT_DIR / "data_manifest.csv"
COVERAGE_PATH = OUT_DIR / "month_coverage.csv"
BRAND_MAP_PATH = ROOT / "data" / "derived" / "brand_map.csv"
VALIDATION_DIR = OUT_DIR / "validation"
GUARDRAIL_PATH = VALIDATION_DIR / "brand_map_guardrails.csv"

TARGET_MONTHS = ["2025-12", "2026-01", "2026-02"]

MONTH_MAP = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


@dataclass
class FileMeta:
    relative_path: str
    source_group: str
    extension: str
    size_bytes: int
    modified_at: str
    parsed_month: str
    parseable_by_etl: str
    header_preview: str
    needs_manual_xlsx_etl: str
    notes: str


def month_from_name(name: str) -> str:
    text = name.lower()
    # e.g. Dec_25 / Jan_26 / Feb 2026
    m = re.search(r"(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[_ \-]?(\d{2,4})", text)
    if not m:
        return ""

    mon = MONTH_MAP[m.group(1)]
    yr_raw = m.group(2)
    year = int(yr_raw) if len(yr_raw) == 4 else int(f"20{yr_raw}")
    return f"{year:04d}-{mon:02d}"


def classify_source(rel_path: str) -> str:
    p = rel_path.lower()
    if "google dashboard" in p:
        return "google_ads_campaign"
    if "google shopify dashboard" in p:
        return "google_shopify"
    if "swfxmetadashboard" in p:
        return "meta_d2c_adset"
    if "swf-metaxshopifydashboard" in p:
        return "meta_shopify_sales"
    if "swf-metaxblinkit" in p:
        return "meta_blinkit"
    if "swf-metaxinstamart" in p:
        return "meta_instamart"
    if "amazon sales_sara_s wholesome" in p:
        return "amazon_sales"
    if "new vs returning" in p:
        return "shopify_new_vs_returning"
    if "volume trend" in p:
        return "brand_search_google"
    if "platformwise spend" in p:
        return "platform_spend"
    return "other"


def file_modified_iso(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")


def read_header_preview(path: Path) -> str:
    if path.suffix.lower() != ".csv":
        return ""

    # Google campaign exports are UTF-16 TSV with two metadata rows.
    encodings = ["utf-8-sig", "utf-16", "latin-1"]
    for enc in encodings:
        try:
            with path.open("r", encoding=enc, newline="") as f:
                lines = [next(f, "").strip() for _ in range(3)]
        except Exception:
            continue

        if len(lines) >= 3 and "campaign report" in lines[0].lower():
            return lines[2][:500]
        for ln in lines:
            if ln:
                return ln[:500]
    return ""


def detect_parseability(path: Path) -> tuple[str, str]:
    ext = path.suffix.lower()
    if ext == ".csv":
        return "yes", ""
    if ext == ".xlsx":
        return "no", "xlsx requires explicit conversion/parse path (no openpyxl dependency yet)"
    return "no", "unsupported extension"


def build_manifest(files: Iterable[Path]) -> list[FileMeta]:
    out: list[FileMeta] = []
    for p in sorted(files):
        rel = str(p.relative_to(ROOT))
        source = classify_source(rel)
        parseable, note = detect_parseability(p)
        header_preview = read_header_preview(p)
        is_xlsx = "yes" if p.suffix.lower() == ".xlsx" else "no"
        out.append(
            FileMeta(
                relative_path=rel,
                source_group=source,
                extension=p.suffix.lower(),
                size_bytes=p.stat().st_size,
                modified_at=file_modified_iso(p),
                parsed_month=month_from_name(p.name),
                parseable_by_etl=parseable,
                header_preview=header_preview,
                needs_manual_xlsx_etl=is_xlsx,
                notes=note,
            )
        )
    return out


def write_manifest(rows: list[FileMeta]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with MANIFEST_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "relative_path",
                "source_group",
                "extension",
                "size_bytes",
                "modified_at",
                "parsed_month",
                "parseable_by_etl",
                "needs_manual_xlsx_etl",
                "header_preview",
                "notes",
            ]
        )
        for r in rows:
            w.writerow(
                [
                    r.relative_path,
                    r.source_group,
                    r.extension,
                    r.size_bytes,
                    r.modified_at,
                    r.parsed_month,
                    r.parseable_by_etl,
                    r.needs_manual_xlsx_etl,
                    r.header_preview,
                    r.notes,
                ]
            )


def write_coverage(rows: list[FileMeta]) -> None:
    monthish_sources = {
        "google_ads_campaign",
        "google_shopify",
        "meta_d2c_adset",
        "meta_shopify_sales",
        "meta_blinkit",
        "meta_instamart",
        "amazon_sales",
    }
    found: dict[tuple[str, str], bool] = {}
    for r in rows:
        if r.source_group not in monthish_sources:
            continue
        if r.parsed_month:
            found[(r.source_group, r.parsed_month)] = True

    with COVERAGE_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["source_group", "month", "status"])
        for src in sorted(monthish_sources):
            for m in TARGET_MONTHS:
                status = "available" if found.get((src, m), False) else "missing"
                w.writerow([src, m, status])


def apply_manifest_status_rules() -> None:
    manifest_df = pd.read_csv(MANIFEST_PATH)
    coverage_df = pd.read_csv(COVERAGE_PATH)

    # Baseline status from parseability.
    manifest_df["status"] = manifest_df["parseable_by_etl"].map(
        {"yes": "received_ready_csv", "no": "received_xlsx_manual_etl"}
    ).fillna("received")
    manifest_df["action_required"] = ""

    # Known file-level validation issue.
    jan_mask = manifest_df["relative_path"].astype(str).str.contains(
        "SWF-ShopifyDashboardXMeta-Jan_25.csv", na=False
    )
    manifest_df.loc[jan_mask, "status"] = "received_needs_validation"
    manifest_df.loc[jan_mask, "action_required"] = (
        "Confirm if this is Jan 2026; re-export if mislabeled/misperiod."
    )

    # Add pending rows from month coverage gaps.
    pending_rows: list[dict[str, str]] = []
    missing_cov = coverage_df[coverage_df["status"] == "missing"]
    for _, row in missing_cov.iterrows():
        src = row["source_group"]
        month = row["month"]
        rel = f"PENDING::{src}::{month}"
        pending_rows.append(
            {
                "relative_path": rel,
                "source_group": src,
                "extension": "",
                "size_bytes": "",
                "modified_at": "",
                "parsed_month": month,
                "parseable_by_etl": "no",
                "needs_manual_xlsx_etl": "no",
                "header_preview": "",
                "notes": "Expected month-wise extract not received yet.",
                "status": "awaited",
                "action_required": "Please share month-wise extract for this source and month.",
            }
        )

    # Explicitly track pending Saras total revenue file.
    pending_rows.append(
        {
            "relative_path": "PENDING::saras_total_revenue_by_channel::2025-12_to_2026-02",
            "source_group": "saras_total_revenue_by_channel",
            "extension": "",
            "size_bytes": "",
            "modified_at": "",
            "parsed_month": "2025-12_to_2026-02",
            "parseable_by_etl": "no",
            "needs_manual_xlsx_etl": "no",
            "header_preview": "",
            "notes": (
                "Pending from data owners: Saras total revenue by month across D2C, "
                "Offline, Q-Com, etc."
            ),
            "status": "awaited",
            "action_required": (
                "Share monthly file for Dec 2025, Jan 2026, Feb 2026 (CSV preferred)."
            ),
        }
    )

    if pending_rows:
        pending_df = pd.DataFrame(pending_rows)
        manifest_df = pd.concat([manifest_df, pending_df], ignore_index=True)
        # Deduplicate by relative path; keep latest status row.
        manifest_df = manifest_df.drop_duplicates(subset=["relative_path"], keep="last")

    order = {
        "awaited": 0,
        "received_needs_validation": 1,
        "received_xlsx_manual_etl": 2,
        "received_ready_csv": 3,
    }
    manifest_df["_sort_order"] = manifest_df["status"].map(order).fillna(9)
    manifest_df = manifest_df.sort_values(
        by=["_sort_order", "source_group", "parsed_month", "relative_path"]
    ).drop(columns=["_sort_order"])

    manifest_df.to_csv(MANIFEST_PATH, index=False)


def read_google_ads_campaign_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, encoding="utf-16", sep="\t", skiprows=2)


def read_standard_csv(path: Path) -> pd.DataFrame:
    # Handles UTF-8 and most simple CSVs in the folder.
    return pd.read_csv(path)


def _safe_read(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def combine_csv_group(
    source_group: str,
    out_name: str,
    read_fn,
    include_months_only: bool = False,
) -> int:
    manifest_df = pd.read_csv(MANIFEST_PATH)
    subset = manifest_df[
        (manifest_df["source_group"] == source_group)
        & (manifest_df["parseable_by_etl"] == "yes")
        & (manifest_df["extension"] == ".csv")
    ].copy()

    if include_months_only:
        subset = subset[subset["parsed_month"].isin(TARGET_MONTHS)]

    frames: list[pd.DataFrame] = []
    for _, row in subset.iterrows():
        p = ROOT / row["relative_path"]
        try:
            df = read_fn(p)
        except Exception:
            continue
        df["month_start"] = row["parsed_month"] if isinstance(row["parsed_month"], str) else ""
        df["source_file"] = row["relative_path"]
        frames.append(df)

    if not frames:
        return 0

    out = pd.concat(frames, ignore_index=True)
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    out.to_csv(STAGING_DIR / out_name, index=False)
    return len(out)


def build_staging() -> dict[str, int]:
    counts: dict[str, int] = {}
    counts["google_ads_campaigns_monthly"] = combine_csv_group(
        source_group="google_ads_campaign",
        out_name="google_ads_campaigns_monthly.csv",
        read_fn=read_google_ads_campaign_csv,
        include_months_only=True,
    )
    counts["google_shopify_monthly"] = combine_csv_group(
        source_group="google_shopify",
        out_name="google_shopify_monthly.csv",
        read_fn=read_standard_csv,
        include_months_only=True,
    )
    counts["meta_d2c_adset_monthly"] = combine_csv_group(
        source_group="meta_d2c_adset",
        out_name="meta_d2c_adset_monthly.csv",
        read_fn=read_standard_csv,
        include_months_only=True,
    )
    counts["meta_blinkit_monthly"] = combine_csv_group(
        source_group="meta_blinkit",
        out_name="meta_blinkit_monthly.csv",
        read_fn=read_standard_csv,
        include_months_only=True,
    )
    counts["meta_instamart_monthly"] = combine_csv_group(
        source_group="meta_instamart",
        out_name="meta_instamart_monthly.csv",
        read_fn=read_standard_csv,
        include_months_only=True,
    )
    counts["meta_shopify_sales_monthly"] = combine_csv_group(
        source_group="meta_shopify_sales",
        out_name="meta_shopify_sales_monthly.csv",
        read_fn=read_standard_csv,
        include_months_only=True,
    )
    counts["shopify_new_vs_returning_daily"] = combine_csv_group(
        source_group="shopify_new_vs_returning",
        out_name="shopify_new_vs_returning_daily.csv",
        read_fn=read_standard_csv,
        include_months_only=False,
    )
    counts["brand_search_google"] = combine_csv_group(
        source_group="brand_search_google",
        out_name="brand_search_google_raw.csv",
        read_fn=read_standard_csv,
        include_months_only=False,
    )
    return counts


def _like_to_regex(pattern: str) -> str:
    esc = re.escape(str(pattern).lower()).replace("%", ".*")
    return f"^{esc}$"


def _build_map_brand_fn():
    if not BRAND_MAP_PATH.exists():
        return None
    bm = pd.read_csv(BRAND_MAP_PATH)
    if bm.empty:
        return None
    bm = bm.copy()
    bm["priority"] = pd.to_numeric(bm.get("priority", 0), errors="coerce").fillna(0).astype(int)
    bm["regex"] = bm["pattern"].astype(str).apply(_like_to_regex)

    def map_brand(name: str, platform: str) -> str | None:
        text = str(name).lower()
        sub = bm[bm["platform"].str.lower() == platform.lower()]
        hits = []
        for _, r in sub.iterrows():
            if re.search(r["regex"], text):
                hits.append((r["brand"], int(r["priority"]), len(str(r["pattern"]))))
        if not hits:
            return None
        hits.sort(key=lambda x: (x[1], x[2]), reverse=True)
        return hits[0][0]

    return map_brand


def run_brand_map_guardrails() -> dict[str, int]:
    map_brand = _build_map_brand_fn()
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)

    if map_brand is None:
        pd.DataFrame(
            [
                {
                    "rule": "brand_map_available",
                    "source": "",
                    "month_start": "",
                    "entity_name": "",
                    "mapped_brand": "",
                    "status": "warning",
                    "details": "brand_map.csv missing or empty; guardrails skipped.",
                }
            ]
        ).to_csv(GUARDRAIL_PATH, index=False)
        return {"violations": 0, "warnings": 1}

    violations: list[dict[str, str]] = []

    def check_df(df: pd.DataFrame, name_col: str, platform: str, source: str) -> None:
        if df.empty or name_col not in df.columns:
            return
        for _, row in df.iterrows():
            name = str(row.get(name_col, ""))
            month = str(row.get("month_start", ""))
            mapped = map_brand(name, platform)
            low = name.lower()

            # Rule 1: wholesome must map to Sara's.
            if "wholesome" in low and mapped != "Sara's":
                violations.append(
                    {
                        "rule": "wholesome_must_map_to_saras",
                        "source": source,
                        "month_start": month,
                        "entity_name": name,
                        "mapped_brand": mapped,
                        "status": "violation",
                        "details": "Entity contains 'wholesome' but did not map to Sara's.",
                    }
                )

            # Rule 2: treats should not map to Sara's.
            if ("treat" in low or "treats" in low) and mapped == "Sara's":
                violations.append(
                    {
                        "rule": "treats_should_not_map_to_saras",
                        "source": source,
                        "month_start": month,
                        "entity_name": name,
                        "mapped_brand": mapped,
                        "status": "violation",
                        "details": "Entity contains treat/treats but mapped to Sara's.",
                    }
                )

    g = _safe_read(STAGING_DIR / "google_ads_campaigns_monthly.csv")
    check_df(g, "Campaign", "google", "google_ads_campaigns_monthly")

    md = _safe_read(STAGING_DIR / "meta_d2c_adset_monthly.csv")
    check_df(md, "Ad set name", "meta", "meta_d2c_adset_monthly")

    mb = _safe_read(STAGING_DIR / "meta_blinkit_monthly.csv")
    check_df(mb, "Ad set name", "meta", "meta_blinkit_monthly")

    mi = _safe_read(STAGING_DIR / "meta_instamart_monthly.csv")
    check_df(mi, "Ad set name", "meta", "meta_instamart_monthly")

    out = pd.DataFrame(violations)
    if out.empty:
        out = pd.DataFrame(
            [
                {
                    "rule": "all",
                    "source": "",
                    "month_start": "",
                    "entity_name": "",
                    "mapped_brand": "",
                    "status": "ok",
                    "details": "No brand-map guardrail violations found.",
                }
            ]
        )
    out.to_csv(GUARDRAIL_PATH, index=False)
    return {
        "violations": int((out["status"] == "violation").sum()),
        "warnings": int((out["status"] == "warning").sum()),
    }


def main() -> None:
    if not DASHBOARD_DATA.exists():
        raise FileNotFoundError(f"Missing folder: {DASHBOARD_DATA}")

    files = [
        p
        for p in DASHBOARD_DATA.rglob("*")
        if p.is_file() and p.name != ".DS_Store"
    ]
    manifest = build_manifest(files)
    write_manifest(manifest)
    write_coverage(manifest)
    apply_manifest_status_rules()
    counts = build_staging()
    guardrail = run_brand_map_guardrails()

    print(f"Wrote manifest: {MANIFEST_PATH}")
    print(f"Wrote month coverage: {COVERAGE_PATH}")
    print(f"Staging dir: {STAGING_DIR}")
    for name, count in counts.items():
        print(f"- {name}: {count} rows")
    print(f"Brand map guardrails: {guardrail['violations']} violations, {guardrail['warnings']} warnings")
    print(f"Guardrail report: {GUARDRAIL_PATH}")


if __name__ == "__main__":
    main()
