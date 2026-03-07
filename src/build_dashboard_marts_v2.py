from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import re


ROOT = Path(__file__).resolve().parents[1]
BASE_DIR = ROOT / "data" / "derived" / "dashboard_v2"
STAGING = BASE_DIR / "staging"
MARTS = BASE_DIR / "marts"
MANIFEST_PATH = BASE_DIR / "data_manifest.csv"
COVERAGE_PATH = BASE_DIR / "month_coverage.csv"
BRAND_MAP_PATH = ROOT / "data" / "derived" / "brand_map.csv"

TARGET_MONTHS = ["2025-12", "2026-01", "2026-02"]


def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(
        s.astype(str).str.replace(",", "", regex=False).str.replace("%", "", regex=False),
        errors="coerce",
    )


def _safe_read(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _build_brand_mapper():
    if not BRAND_MAP_PATH.exists():
        return None
    bm = pd.read_csv(BRAND_MAP_PATH)
    if bm.empty:
        return None

    bm = bm.copy()
    bm["priority"] = pd.to_numeric(bm.get("priority", 0), errors="coerce").fillna(0).astype(int)

    def like_to_regex(pattern: str) -> str:
        esc = re.escape(str(pattern).lower()).replace("%", ".*")
        return f"^{esc}$"

    bm["regex"] = bm["pattern"].astype(str).apply(like_to_regex)

    def mapper(text: str, platform: str) -> str | None:
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

    return mapper


def _filter_meta_shopify_to_saras(ms: pd.DataFrame, map_brand) -> pd.DataFrame:
    out = ms.copy()
    if out.empty:
        return out

    # New revised files provide adset/campaign identifiers via UTM fields.
    if map_brand is not None:
        for col in ["Order UTM content", "Order UTM campaign", "Order UTM medium"]:
            if col in out.columns:
                out[f"mapped_brand_{col}"] = out[col].astype(str).apply(
                    lambda x: map_brand(x, "meta")
                )
        mapped_cols = [c for c in out.columns if c.startswith("mapped_brand_")]
        if mapped_cols:
            mask = False
            for c in mapped_cols:
                mask = mask | (out[c] == "Sara's")
            out = out[mask].copy()

    # Legacy fallback files are product-level; keep Saras/Wholesome titles.
    if "Product title at time of sale" in out.columns:
        t = out["Product title at time of sale"].astype(str).str.lower()
        out = out[t.str.contains("sara|wholesome", regex=True, na=False)].copy()

    return out


def _objective_from_google(campaign_type: str) -> str:
    t = str(campaign_type).lower()
    if any(k in t for k in ["video", "demand gen", "display"]):
        return "demand_generation"
    return "demand_capture"


def _build_conversion_channel_monthly() -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    map_brand = _build_brand_mapper()

    # Google platform layer
    g = _safe_read(STAGING / "google_ads_campaigns_monthly.csv")
    if not g.empty:
        if map_brand is not None and "Campaign" in g.columns:
            g["mapped_brand"] = g["Campaign"].astype(str).apply(lambda x: map_brand(x, "google"))
            g = g[g["mapped_brand"] == "Sara's"].copy()
        g["spend"] = _to_num(g.get("Cost", pd.Series(dtype=float)))
        g["revenue"] = _to_num(g.get("Revenue", pd.Series(dtype=float)))
        g["orders"] = _to_num(g.get("Orders", pd.Series(dtype=float)))
        g["month_start"] = g["month_start"].astype(str)
        g = g[g["month_start"].isin(TARGET_MONTHS)]
        g["objective_class"] = g.get("Campaign type", pd.Series(dtype=str)).apply(_objective_from_google)
        g["channel"] = "google"
        g["sub_channel"] = g.get("Campaign type", pd.Series(dtype=str)).fillna("unknown").astype(str).str.lower()
        gp = (
            g.groupby(["month_start", "channel", "sub_channel", "objective_class"], as_index=False)[
                ["spend", "revenue", "orders"]
            ]
            .sum(min_count=1)
        )
        gp["attribution_layer"] = "platform"
        gp["brand"] = "saras"
        gp["source_table"] = "google_ads_campaigns_monthly"
        rows.append(gp)

    # Google shopify layer
    gs = _safe_read(STAGING / "google_shopify_monthly.csv")
    if not gs.empty:
        if map_brand is not None and "Order UTM campaign" in gs.columns:
            gs["mapped_brand"] = gs["Order UTM campaign"].astype(str).apply(
                lambda x: map_brand(x, "google")
            )
            gs = gs[gs["mapped_brand"] == "Sara's"].copy()
        gs["spend"] = np.nan
        gs["revenue"] = _to_num(gs.get("Total sales", pd.Series(dtype=float)))
        gs["orders"] = _to_num(gs.get("Orders", pd.Series(dtype=float)))
        gs["month_start"] = gs["month_start"].astype(str)
        gs = gs[gs["month_start"].isin(TARGET_MONTHS)]
        gs["channel"] = "google"
        gs["sub_channel"] = gs.get("Order UTM medium", pd.Series(dtype=str)).fillna("unknown").astype(str).str.lower()
        gs["objective_class"] = "demand_capture"
        gss = (
            gs.groupby(["month_start", "channel", "sub_channel", "objective_class"], as_index=False)[
                ["spend", "revenue", "orders"]
            ]
            .sum(min_count=1)
        )
        gss["attribution_layer"] = "shopify"
        gss["brand"] = "saras"
        gss["source_table"] = "google_shopify_monthly"
        rows.append(gss)

    # Meta D2C platform layer
    m = _safe_read(STAGING / "meta_d2c_adset_monthly.csv")
    if not m.empty:
        if map_brand is not None and "Ad set name" in m.columns:
            m["mapped_brand"] = m["Ad set name"].astype(str).apply(lambda x: map_brand(x, "meta"))
            m = m[m["mapped_brand"] == "Sara's"].copy()
        m["spend"] = _to_num(m.get("Amount spent (INR)", pd.Series(dtype=float)))
        m["revenue"] = _to_num(m.get("Results value", pd.Series(dtype=float)))
        m["orders"] = _to_num(m.get("Purchases", pd.Series(dtype=float))).fillna(
            _to_num(m.get("Results", pd.Series(dtype=float)))
        )
        m["month_start"] = m["month_start"].astype(str)
        m = m[m["month_start"].isin(TARGET_MONTHS)]
        m["channel"] = "meta"
        m["sub_channel"] = "d2c"
        m["objective_class"] = "demand_generation"
        mp = (
            m.groupby(["month_start", "channel", "sub_channel", "objective_class"], as_index=False)[
                ["spend", "revenue", "orders"]
            ]
            .sum(min_count=1)
        )
        mp["attribution_layer"] = "platform"
        mp["brand"] = "saras"
        mp["source_table"] = "meta_d2c_adset_monthly"
        rows.append(mp)

    # Meta Shopify layer (no spend in this source)
    ms = _safe_read(STAGING / "meta_shopify_sales_monthly.csv")
    if not ms.empty:
        ms = _filter_meta_shopify_to_saras(ms, map_brand)
        ms["spend"] = np.nan
        ms["revenue"] = _to_num(ms.get("Total sales", pd.Series(dtype=float)))
        ms["orders"] = _to_num(ms.get("Orders", pd.Series(dtype=float)))
        ms["month_start"] = ms["month_start"].astype(str)
        ms = ms[ms["month_start"].isin(TARGET_MONTHS)]
        ms["channel"] = "meta"
        ms["sub_channel"] = "d2c"
        ms["objective_class"] = "demand_generation"
        mss = (
            ms.groupby(["month_start", "channel", "sub_channel", "objective_class"], as_index=False)[
                ["spend", "revenue", "orders"]
            ]
            .sum(min_count=1)
        )
        mss["attribution_layer"] = "shopify"
        mss["brand"] = "saras"
        mss["source_table"] = "meta_shopify_sales_monthly"
        rows.append(mss)

    # Blinkit platform
    b = _safe_read(STAGING / "meta_blinkit_monthly.csv")
    if not b.empty:
        if map_brand is not None and "Ad set name" in b.columns:
            b["mapped_brand"] = b["Ad set name"].astype(str).apply(lambda x: map_brand(x, "meta"))
            b = b[b["mapped_brand"] == "Sara's"].copy()
        b["spend"] = _to_num(b.get("Amount spent (INR)", pd.Series(dtype=float)))
        b["revenue"] = _to_num(b.get("Results value", pd.Series(dtype=float)))
        b["orders"] = _to_num(b.get("Results", pd.Series(dtype=float)))
        b["month_start"] = b["month_start"].astype(str)
        b = b[b["month_start"].isin(TARGET_MONTHS)]
        b["channel"] = "blinkit"
        b["sub_channel"] = "cpa"
        b["objective_class"] = "demand_capture"
        bp = (
            b.groupby(["month_start", "channel", "sub_channel", "objective_class"], as_index=False)[
                ["spend", "revenue", "orders"]
            ]
            .sum(min_count=1)
        )
        bp["attribution_layer"] = "platform"
        bp["brand"] = "saras"
        bp["source_table"] = "meta_blinkit_monthly"
        rows.append(bp)

    # Instamart platform
    i = _safe_read(STAGING / "meta_instamart_monthly.csv")
    if not i.empty:
        if map_brand is not None and "Ad set name" in i.columns:
            i["mapped_brand"] = i["Ad set name"].astype(str).apply(lambda x: map_brand(x, "meta"))
            i = i[i["mapped_brand"] == "Sara's"].copy()
        i["spend"] = _to_num(i.get("Amount spent (INR)", pd.Series(dtype=float)))
        i["revenue"] = _to_num(i.get("Results value", pd.Series(dtype=float)))
        i["orders"] = _to_num(i.get("Results", pd.Series(dtype=float)))
        i["month_start"] = i["month_start"].astype(str)
        i = i[i["month_start"].isin(TARGET_MONTHS)]
        i["channel"] = "instamart"
        i["sub_channel"] = "cpa"
        i["objective_class"] = "demand_capture"
        ip = (
            i.groupby(["month_start", "channel", "sub_channel", "objective_class"], as_index=False)[
                ["spend", "revenue", "orders"]
            ]
            .sum(min_count=1)
        )
        ip["attribution_layer"] = "platform"
        ip["brand"] = "saras"
        ip["source_table"] = "meta_instamart_monthly"
        rows.append(ip)

    if not rows:
        return pd.DataFrame()

    out = pd.concat(rows, ignore_index=True)
    out["roas"] = np.where((out["spend"].notna()) & (out["spend"] > 0), out["revenue"] / out["spend"], np.nan)
    out["cpa"] = np.where((out["orders"].notna()) & (out["orders"] > 0) & out["spend"].notna(), out["spend"] / out["orders"], np.nan)
    out["data_confidence"] = "medium"
    out.loc[out["attribution_layer"] == "shopify", "data_confidence"] = "high"
    # Drop rows that have no signal at all.
    empty_mask = (
        out["spend"].fillna(0).eq(0)
        & out["revenue"].fillna(0).eq(0)
        & out["orders"].fillna(0).eq(0)
    )
    out = out[~empty_mask].copy()
    return out[
        [
            "month_start",
            "brand",
            "channel",
            "sub_channel",
            "objective_class",
            "attribution_layer",
            "spend",
            "revenue",
            "orders",
            "roas",
            "cpa",
            "data_confidence",
            "source_table",
        ]
    ].sort_values(["month_start", "channel", "attribution_layer", "sub_channel"])


def _build_demand_generation_monthly(conv: pd.DataFrame) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []

    # Meta engagement metrics
    m = _safe_read(STAGING / "meta_d2c_adset_monthly.csv")
    if not m.empty:
        m["month_start"] = m["month_start"].astype(str)
        m = m[m["month_start"].isin(TARGET_MONTHS)].copy()
        agg = m.groupby("month_start", as_index=False).agg(
            spend=("Amount spent (INR)", lambda s: _to_num(s).sum()),
            impressions=("Impressions", lambda s: _to_num(s).sum()),
            reach=("Reach", lambda s: _to_num(s).sum()),
        )
        for metric in ["spend", "impressions", "reach"]:
            x = agg[["month_start", metric]].copy()
            x["brand"] = "saras"
            x["channel"] = "meta"
            x["metric_name"] = metric
            x["metric_value"] = x[metric]
            x["attribution_layer"] = "platform"
            x["source_table"] = "meta_d2c_adset_monthly"
            rows.append(x[["month_start", "brand", "channel", "metric_name", "metric_value", "attribution_layer", "source_table"]])

    # Meta-Shopify new customer trend (month-wise).
    ms = _safe_read(STAGING / "meta_shopify_sales_monthly.csv")
    if not ms.empty and "New customers" in ms.columns:
        ms = ms.copy()
        ms = _filter_meta_shopify_to_saras(ms, _build_brand_mapper())
        ms["month_start"] = ms.get("month_start", pd.Series(dtype=str)).astype(str)
        ms = ms[ms["month_start"].isin(TARGET_MONTHS)].copy()
        if not ms.empty:
            agg = ms.groupby("month_start", as_index=False).agg(
                new_customers=("New customers", lambda s: _to_num(s).sum(min_count=1))
            )
            x = agg[["month_start", "new_customers"]].copy()
            x["brand"] = "saras"
            x["channel"] = "meta"
            x["metric_name"] = "new_customers"
            x["metric_value"] = x["new_customers"]
            x["attribution_layer"] = "shopify"
            x["source_table"] = "meta_shopify_sales_monthly"
            rows.append(
                x[
                    [
                        "month_start",
                        "brand",
                        "channel",
                        "metric_name",
                        "metric_value",
                        "attribution_layer",
                        "source_table",
                    ]
                ]
            )

    # Branded search trend (Google): keep Sara Food row only
    s = _safe_read(STAGING / "brand_search_google_raw.csv")
    if not s.empty and "Unnamed: 0" in s.columns:
        s = s.rename(columns={"Unnamed: 0": "brand_label"})
        sara = s[s["brand_label"].astype(str).str.lower().str.contains("sara")]
        if not sara.empty:
            mapping = {"Dec 2025": "2025-12", "Jan 2026": "2026-01", "Feb 2026": "2026-02"}
            for col, month in mapping.items():
                if col not in sara.columns:
                    val = np.nan
                else:
                    val = _to_num(sara[col]).sum(min_count=1)
                rows.append(
                    pd.DataFrame(
                        {
                            "month_start": [month],
                            "brand": ["saras"],
                            "channel": ["google"],
                            "metric_name": ["branded_search_volume"],
                            "metric_value": [val],
                            "attribution_layer": ["blended"],
                            "source_table": ["brand_search_google_raw"],
                        }
                    )
                )

    # Shopify D2C new vs repeat customer trend (month-wise).
    nvr = _safe_read(STAGING / "shopify_new_vs_returning_daily.csv")
    if not nvr.empty and "New or returning customer" in nvr.columns:
        nvr = nvr.copy()
        # Source file can contain multiple months; derive month from day-level date.
        if "Day" in nvr.columns:
            day_dt = pd.to_datetime(nvr["Day"], errors="coerce")
            nvr["month_start"] = day_dt.dt.strftime("%Y-%m")
        else:
            nvr["month_start"] = nvr.get("month_start", pd.Series(dtype=str)).astype(str)
        nvr = nvr[nvr["month_start"].isin(TARGET_MONTHS)].copy()
        if not nvr.empty:
            nvr["customer_bucket"] = (
                nvr["New or returning customer"].astype(str).str.strip().str.lower()
            )
            nvr["customers"] = _to_num(nvr.get("Customers", pd.Series(dtype=float)))
            agg = (
                nvr.groupby(["month_start", "customer_bucket"], as_index=False)["customers"]
                .sum(min_count=1)
            )
            metric_map = {"new": "d2c_new_customers", "returning": "d2c_repeat_customers"}
            agg["metric_name"] = agg["customer_bucket"].map(metric_map)
            agg = agg[agg["metric_name"].notna()].copy()
            if not agg.empty:
                x = agg[["month_start", "metric_name", "customers"]].copy()
                x["brand"] = "saras"
                x["channel"] = "shopify_d2c"
                x["metric_value"] = x["customers"]
                x["attribution_layer"] = "shopify"
                x["source_table"] = "shopify_new_vs_returning_daily"
                rows.append(
                    x[
                        [
                            "month_start",
                            "brand",
                            "channel",
                            "metric_name",
                            "metric_value",
                            "attribution_layer",
                            "source_table",
                        ]
                    ]
                )

    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    return out.sort_values(["month_start", "channel", "metric_name"])


def _build_reconciliation_monthly(conv: pd.DataFrame) -> pd.DataFrame:
    if conv.empty:
        return pd.DataFrame()
    tmp = conv[conv["channel"].isin(["google", "meta"])].copy()
    pivot = (
        tmp.groupby(["month_start", "channel", "attribution_layer"], as_index=False)[["spend", "revenue"]]
        .sum(min_count=1)
        .pivot_table(index=["month_start", "channel"], columns="attribution_layer", values=["spend", "revenue"], aggfunc="sum")
    )
    pivot.columns = ["_".join([c for c in col if c]) for col in pivot.columns.to_flat_index()]
    pivot = pivot.reset_index()
    for c in ["revenue_platform", "revenue_shopify", "spend_platform"]:
        if c not in pivot.columns:
            pivot[c] = np.nan
    pivot["roas_platform"] = pivot["revenue_platform"] / pivot["spend_platform"]
    pivot["roas_shopify"] = pivot["revenue_shopify"] / pivot["spend_platform"]
    pivot["revenue_delta"] = pivot["revenue_platform"] - pivot["revenue_shopify"]
    pivot["roas_delta"] = pivot["roas_platform"] - pivot["roas_shopify"]
    pivot["brand"] = "saras"
    pivot["status"] = "ok"
    pivot.loc[pivot["revenue_shopify"].isna(), "status"] = "missing_shopify_layer"
    return pivot[
        [
            "month_start",
            "brand",
            "channel",
            "spend_platform",
            "revenue_platform",
            "revenue_shopify",
            "roas_platform",
            "roas_shopify",
            "revenue_delta",
            "roas_delta",
            "status",
        ]
    ].sort_values(["month_start", "channel"])


def _build_exec_summary_monthly(conv: pd.DataFrame) -> pd.DataFrame:
    if conv.empty:
        return pd.DataFrame()
    tmp = conv.groupby(["month_start", "attribution_layer"], as_index=False)[["spend", "revenue"]].sum(min_count=1)
    piv = tmp.pivot_table(index="month_start", columns="attribution_layer", values=["spend", "revenue"], aggfunc="sum")
    piv.columns = ["_".join([c for c in col if c]) for col in piv.columns.to_flat_index()]
    piv = piv.reset_index()
    for col in ["spend_platform", "revenue_platform", "revenue_shopify", "revenue_blended"]:
        if col not in piv.columns:
            piv[col] = np.nan
    piv["paid_spend_platform"] = piv["spend_platform"]
    piv["paid_revenue_platform"] = piv["revenue_platform"]
    piv["paid_revenue_shopify"] = piv["revenue_shopify"]
    piv["paid_roas_platform"] = piv["paid_revenue_platform"] / piv["paid_spend_platform"]
    piv["paid_roas_shopify"] = piv["paid_revenue_shopify"] / piv["paid_spend_platform"]
    # Awaited input.
    piv["total_revenue_all_channels"] = np.nan
    piv["mer"] = np.nan
    piv["brand"] = "saras"
    piv["status_notes"] = "Pending Saras total revenue by channel file."
    return piv[
        [
            "month_start",
            "brand",
            "paid_spend_platform",
            "paid_revenue_platform",
            "paid_revenue_shopify",
            "paid_roas_platform",
            "paid_roas_shopify",
            "total_revenue_all_channels",
            "mer",
            "status_notes",
        ]
    ].sort_values("month_start")


def _build_data_quality_issues() -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    if COVERAGE_PATH.exists():
        cov = pd.read_csv(COVERAGE_PATH)
        miss = cov[cov["status"] == "missing"]
        for _, r in miss.iterrows():
            rows.append(
                {
                    "issue_type": "missing_source_month",
                    "source_group": str(r["source_group"]),
                    "month_start": str(r["month"]),
                    "severity": "high",
                    "details": "Expected month-wise extract not received.",
                }
            )
    if MANIFEST_PATH.exists():
        m = pd.read_csv(MANIFEST_PATH)
        suspect = m[m["relative_path"].astype(str).str.contains("Jan_25.csv", na=False)]
        for _, r in suspect.iterrows():
            rows.append(
                {
                    "issue_type": "file_validation",
                    "source_group": str(r.get("source_group", "")),
                    "month_start": "2026-01",
                    "severity": "high",
                    "details": "Filename suggests Jan_25; confirm if Jan_26 extract.",
                }
            )
        awaited = m[m.get("status", pd.Series(dtype=str)) == "awaited"]
        for _, r in awaited.iterrows():
            rows.append(
                {
                    "issue_type": "awaited_file",
                    "source_group": str(r.get("source_group", "")),
                    "month_start": str(r.get("parsed_month", "")),
                    "severity": "high",
                    "details": str(r.get("action_required", "")),
                }
            )
    return pd.DataFrame(rows)


def main() -> None:
    MARTS.mkdir(parents=True, exist_ok=True)

    conv = _build_conversion_channel_monthly()
    if not conv.empty:
        conv.to_csv(MARTS / "mart_conversion_channel_monthly.csv", index=False)
        conv[conv["objective_class"] == "demand_capture"].to_csv(
            MARTS / "mart_demand_capture_monthly.csv", index=False
        )
    else:
        pd.DataFrame().to_csv(MARTS / "mart_conversion_channel_monthly.csv", index=False)
        pd.DataFrame().to_csv(MARTS / "mart_demand_capture_monthly.csv", index=False)

    dg = _build_demand_generation_monthly(conv)
    dg.to_csv(MARTS / "mart_demand_generation_monthly.csv", index=False)

    recon = _build_reconciliation_monthly(conv)
    recon.to_csv(MARTS / "mart_reconciliation_monthly.csv", index=False)

    exec_sum = _build_exec_summary_monthly(conv)
    exec_sum.to_csv(MARTS / "mart_exec_summary_monthly.csv", index=False)

    dqi = _build_data_quality_issues()
    dqi.to_csv(MARTS / "mart_data_quality_issues.csv", index=False)

    print(f"Wrote marts to: {MARTS}")
    for p in sorted(MARTS.glob("*.csv")):
        print("-", p.name, "rows:", len(pd.read_csv(p)))


if __name__ == "__main__":
    main()
