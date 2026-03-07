from __future__ import annotations

from pathlib import Path

import pandas as pd
import re


ROOT = Path(__file__).resolve().parents[1]
MARTS_DIR = ROOT / "data" / "derived" / "dashboard_v2" / "marts"
STAGING_DIR = ROOT / "data" / "derived" / "dashboard_v2" / "staging"
BRAND_MAP_PATH = ROOT / "data" / "derived" / "brand_map.csv"
OUT_DIR = ROOT / "data" / "derived" / "dashboard_v2" / "dashboard_v1"
CHANNEL_LABEL_MAP = {
    "blinkit": "blinkit_cpass",
    "instamart": "instamart_cpass",
}
GOOGLE_CAMPAIGN_KEY_ALIASES = {
    "pmax_sara_s_new_customer": "pmax_sara_s_wholesome_new_customer",
}


def _read_csv(name: str) -> pd.DataFrame:
    p = MARTS_DIR / name
    if not p.exists():
        return pd.DataFrame()
    return pd.read_csv(p)


def _fmt(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        if out[c].dtype.kind in "f":
            out[c] = out[c].round(2)
    return out


def _apply_dashboard_channel_labels(df: pd.DataFrame, col: str = "channel") -> pd.DataFrame:
    out = df.copy()
    if col in out.columns:
        out[col] = out[col].astype(str).replace(CHANNEL_LABEL_MAP)
    return out


def _like_to_regex(pattern: str) -> str:
    esc = re.escape(str(pattern).lower()).replace("%", ".*")
    return f"^{esc}$"


def _campaign_key(value: str) -> str:
    s = str(value).lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")


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


def _to_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False).str.replace("%", "", regex=False),
        errors="coerce",
    )


def _build_tab6_campaign_brand_mapping() -> pd.DataFrame:
    map_brand = _build_map_brand_fn()
    if map_brand is None:
        return pd.DataFrame(
            columns=[
                "month_start",
                "platform",
                "source",
                "campaign_name",
                "mapped_brand",
                "attribution_setting",
                "spend",
            ]
        )

    rows = []

    # Google campaigns
    g_path = STAGING_DIR / "google_ads_campaigns_monthly.csv"
    if g_path.exists():
        g = pd.read_csv(g_path)
        if {"Campaign", "month_start", "Cost"}.issubset(g.columns):
            g = g[["Campaign", "month_start", "Cost"]].copy()
            g["spend"] = _to_num(g["Cost"])
            g["campaign_name"] = g["Campaign"].astype(str)
            g["mapped_brand"] = g["campaign_name"].apply(lambda x: map_brand(x, "google"))
            g["attribution_setting"] = ""
            g["platform"] = "google"
            g["source"] = "google_ads_campaigns_monthly"
            rows.append(
                g[
                    [
                        "month_start",
                        "platform",
                        "source",
                        "campaign_name",
                        "mapped_brand",
                        "attribution_setting",
                        "spend",
                    ]
                ]
            )

    # Meta D2C ad sets
    m_path = STAGING_DIR / "meta_d2c_adset_monthly.csv"
    if m_path.exists():
        m = pd.read_csv(m_path)
        if {"Ad set name", "month_start", "Amount spent (INR)"}.issubset(m.columns):
            keep_cols = ["Ad set name", "month_start", "Amount spent (INR)"]
            if "Attribution setting" in m.columns:
                keep_cols.append("Attribution setting")
            m = m[keep_cols].copy()
            m["spend"] = _to_num(m["Amount spent (INR)"])
            m["campaign_name"] = m["Ad set name"].astype(str)
            m["mapped_brand"] = m["campaign_name"].apply(lambda x: map_brand(x, "meta"))
            m["attribution_setting"] = m.get("Attribution setting", pd.Series(dtype=str)).fillna("").astype(str)
            m["platform"] = "meta"
            m["source"] = "meta_d2c_adset_monthly"
            rows.append(
                m[
                    [
                        "month_start",
                        "platform",
                        "source",
                        "campaign_name",
                        "mapped_brand",
                        "attribution_setting",
                        "spend",
                    ]
                ]
            )

    if not rows:
        return pd.DataFrame(
            columns=[
                "month_start",
                "platform",
                "source",
                "campaign_name",
                "mapped_brand",
                "attribution_setting",
                "spend",
            ]
        )

    out = pd.concat(rows, ignore_index=True)
    out = (
        out.groupby(
            [
                "month_start",
                "platform",
                "source",
                "campaign_name",
                "mapped_brand",
                "attribution_setting",
            ],
            as_index=False,
        )[
            "spend"
        ]
        .sum(min_count=1)
        .sort_values(["month_start", "platform", "spend"], ascending=[True, True, False])
    )
    out = out[out["spend"].fillna(0) != 0].copy()
    return out


def _build_tab1(exec_df: pd.DataFrame, conv_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    summary = exec_df.copy()
    if not summary.empty:
        summary = summary.rename(
            columns={
                "paid_spend_platform": "paid_spend",
                "paid_revenue_platform": "paid_revenue_paid_attribution",
                "paid_revenue_shopify": "paid_revenue_shopify_attribution",
            }
        )
    if not conv_df.empty:
        spend = conv_df[conv_df["attribution_layer"] == "platform"].copy()
        mix = (
            spend.groupby(["month_start", "channel"], as_index=False)["spend"]
            .sum(min_count=1)
            .fillna(0)
        )
        month_tot = mix.groupby("month_start", as_index=False)["spend"].sum().rename(
            columns={"spend": "month_spend_total"}
        )
        mix = mix.merge(month_tot, on="month_start", how="left")
        mix["channel_spend_pct"] = (mix["spend"] / mix["month_spend_total"]) * 100
        mix = _apply_dashboard_channel_labels(mix, "channel")
    else:
        mix = pd.DataFrame(columns=["month_start", "channel", "spend", "month_spend_total", "channel_spend_pct"])
    return summary, mix


def _build_tab2_conversion_insights(conv_df: pd.DataFrame) -> pd.DataFrame:
    """Head-of-performance view:
    monthly x channel x objective with platform vs shopify attributed outcomes side-by-side.
    """
    if conv_df.empty:
        return pd.DataFrame(
            columns=[
                "month_start",
                "channel",
                "paid_spend",
                "orders_platform",
                "cpa_platform",
                "revenue_paid_attribution",
                "revenue_shopify_attribution",
                "roas_paid_attribution",
                "roas_shopify_attribution",
                "revenue_gap_paid_vs_shopify",
                "roas_paid_over_shopify",
            ]
        )

    d = conv_df.copy()
    for c in ["spend", "revenue", "orders"]:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")

    grp = (
        d.groupby(["month_start", "channel", "attribution_layer"], as_index=False)[
            ["spend", "revenue", "orders"]
        ]
        .sum(min_count=1)
    )
    piv = grp.pivot_table(
        index=["month_start", "channel"],
        columns="attribution_layer",
        values=["spend", "revenue", "orders"],
        aggfunc="sum",
    )
    piv.columns = ["_".join([x for x in col if x]).strip("_") for col in piv.columns.to_flat_index()]
    out = piv.reset_index()

    # Standardize business column names.
    out["paid_spend"] = out.get("spend_platform")
    out["orders_platform"] = out.get("orders_platform")
    out["revenue_paid_attribution"] = out.get("revenue_platform")
    out["revenue_shopify_attribution"] = out.get("revenue_shopify")
    out["roas_paid_attribution"] = out["revenue_paid_attribution"] / out["paid_spend"]
    out["roas_shopify_attribution"] = out["revenue_shopify_attribution"] / out["paid_spend"]
    out["cpa_platform"] = out["paid_spend"] / out["orders_platform"]
    out["revenue_gap_paid_vs_shopify"] = (
        out["revenue_paid_attribution"] - out["revenue_shopify_attribution"]
    )
    out["roas_paid_over_shopify"] = out["roas_paid_attribution"] / out["roas_shopify_attribution"]

    keep = [
        "month_start",
        "channel",
        "paid_spend",
        "orders_platform",
        "cpa_platform",
        "revenue_paid_attribution",
        "revenue_shopify_attribution",
        "roas_paid_attribution",
        "roas_shopify_attribution",
        "revenue_gap_paid_vs_shopify",
        "roas_paid_over_shopify",
    ]
    out = out[keep].sort_values(["month_start", "channel"])
    out = _apply_dashboard_channel_labels(out, "channel")
    return out


def _safe_div(num: pd.Series, den: pd.Series) -> pd.Series:
    den_num = pd.to_numeric(den, errors="coerce")
    num_num = pd.to_numeric(num, errors="coerce")
    return num_num / den_num.where(den_num != 0)


def _build_tab2_google_campaign_drilldown() -> pd.DataFrame:
    cols = [
        "month_start",
        "campaign_name",
        "campaign_type",
        "paid_spend",
        "orders_platform",
        "cpa_platform",
        "revenue_paid_attribution",
        "revenue_shopify_attribution",
        "roas_paid_attribution",
        "roas_shopify_attribution",
        "revenue_gap_paid_vs_shopify",
        "roas_paid_over_shopify",
    ]
    map_brand = _build_map_brand_fn()

    g_path = STAGING_DIR / "google_ads_campaigns_monthly.csv"
    if not g_path.exists():
        return pd.DataFrame(columns=cols)

    gp = pd.read_csv(g_path)
    if not {"month_start", "Campaign"}.issubset(gp.columns):
        return pd.DataFrame(columns=cols)
    if map_brand is not None:
        gp["mapped_brand"] = gp["Campaign"].astype(str).apply(lambda x: map_brand(x, "google"))
        gp = gp[gp["mapped_brand"] == "Sara's"].copy()
    gp["paid_spend"] = _to_num(gp.get("Cost", pd.Series(dtype=float)))
    gp["revenue_paid_attribution"] = _to_num(gp.get("Revenue", pd.Series(dtype=float)))
    gp["orders_platform"] = _to_num(gp.get("Orders", pd.Series(dtype=float)))
    gp["campaign_name"] = gp["Campaign"].astype(str)
    gp["campaign_type"] = gp.get("Campaign type", pd.Series(dtype=str)).fillna("").astype(str)
    gp["campaign_key"] = gp["campaign_name"].apply(_campaign_key)
    gp = (
        gp.groupby(["month_start", "campaign_name", "campaign_type", "campaign_key"], as_index=False)[
            ["paid_spend", "revenue_paid_attribution", "orders_platform"]
        ]
        .sum(min_count=1)
    )

    gs_path = STAGING_DIR / "google_shopify_monthly.csv"
    if gs_path.exists():
        gs = pd.read_csv(gs_path)
        if {"month_start", "Order UTM campaign"}.issubset(gs.columns):
            if map_brand is not None:
                gs["mapped_brand"] = gs["Order UTM campaign"].astype(str).apply(
                    lambda x: map_brand(x, "google")
                )
                gs = gs[gs["mapped_brand"] == "Sara's"].copy()
            gs["campaign_name"] = gs["Order UTM campaign"].astype(str)
            gs["campaign_key"] = gs["campaign_name"].apply(_campaign_key).replace(
                GOOGLE_CAMPAIGN_KEY_ALIASES
            )
            gs["revenue_shopify_attribution"] = _to_num(gs.get("Total sales", pd.Series(dtype=float)))
            gs = (
                gs.groupby(["month_start", "campaign_key"], as_index=False)[
                    ["revenue_shopify_attribution"]
                ]
                .sum(min_count=1)
            )
            out = gp.merge(gs, on=["month_start", "campaign_key"], how="left")
        else:
            out = gp.copy()
            out["revenue_shopify_attribution"] = pd.NA
    else:
        out = gp.copy()
        out["revenue_shopify_attribution"] = pd.NA

    out["campaign_type"] = out.get("campaign_type", "").fillna("")
    out["cpa_platform"] = _safe_div(out["paid_spend"], out["orders_platform"])
    out["roas_paid_attribution"] = _safe_div(out["revenue_paid_attribution"], out["paid_spend"])
    out["roas_shopify_attribution"] = _safe_div(out["revenue_shopify_attribution"], out["paid_spend"])
    out["revenue_gap_paid_vs_shopify"] = (
        pd.to_numeric(out["revenue_paid_attribution"], errors="coerce")
        - pd.to_numeric(out["revenue_shopify_attribution"], errors="coerce")
    )
    out["roas_paid_over_shopify"] = _safe_div(
        out["roas_paid_attribution"], out["roas_shopify_attribution"]
    )
    out = out.drop(columns=["campaign_key"], errors="ignore")
    out = out[cols].copy()
    out = out[pd.to_numeric(out["paid_spend"], errors="coerce").fillna(0) > 0].copy()

    metric_cols = [
        "paid_spend",
        "orders_platform",
        "revenue_paid_attribution",
        "revenue_shopify_attribution",
    ]
    out = out[out[metric_cols].fillna(0).sum(axis=1) != 0].copy()
    return out.sort_values(["month_start", "paid_spend"], ascending=[True, False])


def _build_tab2_meta_campaign_drilldown() -> pd.DataFrame:
    cols = [
        "month_start",
        "campaign_name",
        "attribution_setting",
        "paid_spend",
        "orders_platform",
        "cpa_platform",
        "revenue_paid_attribution",
        "revenue_shopify_attribution",
        "roas_paid_attribution",
        "roas_shopify_attribution",
        "revenue_gap_paid_vs_shopify",
        "roas_paid_over_shopify",
    ]
    map_brand = _build_map_brand_fn()

    m_path = STAGING_DIR / "meta_d2c_adset_monthly.csv"
    if not m_path.exists():
        return pd.DataFrame(columns=cols)

    m = pd.read_csv(m_path)
    if not {"month_start", "Ad set name"}.issubset(m.columns):
        return pd.DataFrame(columns=cols)
    if map_brand is not None:
        m["mapped_brand"] = m["Ad set name"].astype(str).apply(lambda x: map_brand(x, "meta"))
        m = m[m["mapped_brand"] == "Sara's"].copy()
    m["campaign_name"] = m["Ad set name"].astype(str)
    m["attribution_setting"] = m.get("Attribution setting", pd.Series(dtype=str)).fillna("").astype(str)
    m["paid_spend"] = _to_num(m.get("Amount spent (INR)", pd.Series(dtype=float)))
    m["revenue_paid_attribution"] = _to_num(m.get("Results value", pd.Series(dtype=float)))
    m["orders_platform"] = _to_num(m.get("Purchases", pd.Series(dtype=float))).fillna(
        _to_num(m.get("Results", pd.Series(dtype=float)))
    )
    out = (
        m.groupby(["month_start", "campaign_name", "attribution_setting"], as_index=False)[
            ["paid_spend", "revenue_paid_attribution", "orders_platform"]
        ]
        .sum(min_count=1)
    )
    out["campaign_key"] = out["campaign_name"].apply(_campaign_key)

    ms_path = STAGING_DIR / "meta_shopify_sales_monthly.csv"
    if ms_path.exists():
        ms = pd.read_csv(ms_path)
        key_col = None
        for candidate in ["Order UTM medium", "Order UTM content", "Order UTM campaign"]:
            if candidate in ms.columns:
                key_col = candidate
                break
        if key_col is not None and "month_start" in ms.columns:
            if map_brand is not None:
                ms["mapped_brand"] = ms[key_col].astype(str).apply(
                    lambda x: map_brand(x, "meta")
                )
                ms = ms[ms["mapped_brand"] == "Sara's"].copy()
            ms["campaign_key"] = ms[key_col].astype(str).apply(_campaign_key)
            ms["revenue_shopify_attribution"] = _to_num(ms.get("Total sales", pd.Series(dtype=float)))
            ms = (
                ms.groupby(["month_start", "campaign_key"], as_index=False)[
                    ["revenue_shopify_attribution"]
                ]
                .sum(min_count=1)
            )
            out = out.merge(ms, on=["month_start", "campaign_key"], how="left")
        else:
            out["revenue_shopify_attribution"] = pd.NA
    else:
        out["revenue_shopify_attribution"] = pd.NA

    out["cpa_platform"] = _safe_div(out["paid_spend"], out["orders_platform"])
    out["roas_paid_attribution"] = _safe_div(out["revenue_paid_attribution"], out["paid_spend"])
    out["roas_shopify_attribution"] = _safe_div(out["revenue_shopify_attribution"], out["paid_spend"])
    out["revenue_gap_paid_vs_shopify"] = (
        pd.to_numeric(out["revenue_paid_attribution"], errors="coerce")
        - pd.to_numeric(out["revenue_shopify_attribution"], errors="coerce")
    )
    out["roas_paid_over_shopify"] = _safe_div(
        out["roas_paid_attribution"], out["roas_shopify_attribution"]
    )
    out = out.drop(columns=["campaign_key"], errors="ignore")
    out = out[cols].copy()
    out = out[pd.to_numeric(out["paid_spend"], errors="coerce").fillna(0) > 0].copy()

    metric_cols = ["paid_spend", "orders_platform", "revenue_paid_attribution"]
    out = out[out[metric_cols].fillna(0).sum(axis=1) != 0].copy()
    return out.sort_values(["month_start", "paid_spend"], ascending=[True, False])


def _build_tab3_demand_capture_view(capture_df: pd.DataFrame) -> pd.DataFrame:
    if capture_df.empty:
        return pd.DataFrame(
            columns=[
                "month_start",
                "channel",
                "paid_spend",
                "revenue_paid_attribution",
                "revenue_shopify_attribution",
                "roas_paid_attribution",
                "roas_shopify_attribution",
            ]
        )

    d = capture_df.copy()
    for c in ["spend", "revenue", "orders"]:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")

    grp = (
        d.groupby(["month_start", "channel", "attribution_layer"], as_index=False)[
            ["spend", "revenue", "orders"]
        ]
        .sum(min_count=1)
    )
    piv = grp.pivot_table(
        index=["month_start", "channel"],
        columns="attribution_layer",
        values=["spend", "revenue"],
        aggfunc="sum",
    )
    piv.columns = ["_".join([x for x in col if x]).strip("_") for col in piv.columns.to_flat_index()]
    out = piv.reset_index()

    out["paid_spend"] = out.get("spend_platform")
    out["revenue_paid_attribution"] = out.get("revenue_platform")
    out["revenue_shopify_attribution"] = out.get("revenue_shopify")
    out["roas_paid_attribution"] = out["revenue_paid_attribution"] / out["paid_spend"]
    out["roas_shopify_attribution"] = out["revenue_shopify_attribution"] / out["paid_spend"]

    keep = [
        "month_start",
        "channel",
        "paid_spend",
        "revenue_paid_attribution",
        "revenue_shopify_attribution",
        "roas_paid_attribution",
        "roas_shopify_attribution",
    ]
    out = out[keep].sort_values(["month_start", "channel"])
    out = _apply_dashboard_channel_labels(out, "channel")
    return out


def _build_tab4_demand_generation_view(gen_df: pd.DataFrame) -> pd.DataFrame:
    if gen_df.empty:
        return pd.DataFrame(
            columns=[
                "month_start",
                "meta_spend",
                "meta_impressions",
                "meta_reach",
                "google_branded_search_volume",
                "Meta_New",
                "D2C_New",
                "D2C_Repeat",
            ]
        )

    d = gen_df.copy()
    d["metric_value"] = pd.to_numeric(d.get("metric_value"), errors="coerce")
    d["metric_key"] = d["channel"].astype(str).str.lower() + "_" + d["metric_name"].astype(str).str.lower()
    piv = d.pivot_table(
        index="month_start",
        columns="metric_key",
        values="metric_value",
        aggfunc="first",
    ).reset_index()

    out = pd.DataFrame()
    out["month_start"] = piv["month_start"]
    out["meta_spend"] = piv.get("meta_spend")
    out["meta_impressions"] = piv.get("meta_impressions")
    out["meta_reach"] = piv.get("meta_reach")
    out["Meta_New"] = piv.get("meta_new_customers")
    out["google_branded_search_volume"] = piv.get("google_branded_search_volume")
    out["D2C_New"] = piv.get("shopify_d2c_d2c_new_customers")
    out["D2C_Repeat"] = piv.get("shopify_d2c_d2c_repeat_customers")
    return out.sort_values("month_start")


def _build_tab7_incremental_roas(tab2_df: pd.DataFrame) -> pd.DataFrame:
    """Approximate incremental ROAS using month-on-month deltas.
    iROAS = delta_revenue / delta_spend
    """
    if tab2_df.empty:
        return pd.DataFrame(
            columns=[
                "month_start",
                "channel",
                "paid_spend",
                "revenue_paid_attribution",
                "revenue_shopify_attribution",
                "delta_spend",
                "delta_revenue_paid_attribution",
                "delta_revenue_shopify_attribution",
                "iroas_paid_attribution",
                "iroas_shopify_attribution",
                "data_points_used",
                "notes",
            ]
        )

    d = tab2_df.copy()
    d["month_start"] = d["month_start"].astype(str)
    for c in [
        "paid_spend",
        "revenue_paid_attribution",
        "revenue_shopify_attribution",
    ]:
        d[c] = pd.to_numeric(d[c], errors="coerce")

    d = d.sort_values(["channel", "month_start"]).copy()

    d["delta_spend"] = d.groupby("channel")["paid_spend"].diff()
    d["delta_revenue_paid_attribution"] = d.groupby("channel")["revenue_paid_attribution"].diff()
    d["delta_revenue_shopify_attribution"] = d.groupby("channel")["revenue_shopify_attribution"].diff()

    d["iroas_paid_attribution"] = d["delta_revenue_paid_attribution"] / d["delta_spend"]
    d["iroas_shopify_attribution"] = d["delta_revenue_shopify_attribution"] / d["delta_spend"]

    # Small sample caveat; we only have ~3 months so iROAS is directional.
    d["data_points_used"] = d.groupby("channel").cumcount() + 1
    d["notes"] = "Directional only: month-on-month proxy with limited data points."

    keep = [
        "month_start",
        "channel",
        "paid_spend",
        "revenue_paid_attribution",
        "revenue_shopify_attribution",
        "delta_spend",
        "delta_revenue_paid_attribution",
        "delta_revenue_shopify_attribution",
        "iroas_paid_attribution",
        "iroas_shopify_attribution",
        "data_points_used",
        "notes",
    ]
    return d[keep].sort_values(["month_start", "channel"])


def _to_html_table(df: pd.DataFrame, title: str, table_id: str) -> str:
    if df.empty:
        return f"<h3>{title}</h3><p>No data available.</p>"
    table_html = _fmt(df).to_html(index=False, classes="tbl interactive", table_id=table_id)
    return (
        f"<h3>{title}</h3>"
        f"<div class='table-tools'><button type='button' onclick=\"downloadTableCsv('{table_id}')\">Download CSV</button></div>"
        + table_html
    )


def build_dashboard_html(
    tab1_summary: pd.DataFrame,
    tab1_mix: pd.DataFrame,
    tab2: pd.DataFrame,
    tab2_google_campaign: pd.DataFrame,
    tab2_meta_campaign: pd.DataFrame,
    tab3: pd.DataFrame,
    tab4: pd.DataFrame,
    tab5_recon: pd.DataFrame,
    tab5_dq: pd.DataFrame,
    tab6_map: pd.DataFrame,
) -> str:
    style = """
<style>
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif; margin: 24px; color: #1f2937; }
h1 { margin-bottom: 8px; }
h2 { margin-top: 28px; border-bottom: 1px solid #e5e7eb; padding-bottom: 6px; }
.meta { color: #4b5563; margin-bottom: 18px; }
.tbl { border-collapse: collapse; width: 100%; margin-bottom: 16px; font-size: 12px; }
.tbl th, .tbl td { border: 1px solid #e5e7eb; padding: 6px 8px; text-align: left; }
.tbl th { background: #f3f4f6; position: sticky; top: 0; cursor: pointer; user-select: none; }
.toc a { margin-right: 14px; font-size: 13px; }
.warn { background: #fff7ed; border: 1px solid #fed7aa; padding: 10px; margin-bottom: 14px; }
.tbl .filter-row th { background: #f9fafb; cursor: default; }
.tbl .filter-row input { width: 95%; font-size: 11px; padding: 4px 6px; border: 1px solid #d1d5db; border-radius: 4px; }
.sort-ind { color: #6b7280; font-size: 10px; margin-left: 4px; }
.tbl tr.selected-row { background: #ecfeff; }
.selection-summary { font-size: 12px; color: #374151; margin: -8px 0 16px 0; }
.selection-summary code { background: #f3f4f6; padding: 1px 4px; border-radius: 4px; margin-right: 8px; }
.table-tools { margin: 6px 0 8px 0; }
.table-tools button { font-size: 12px; padding: 6px 10px; border: 1px solid #d1d5db; background: #fff; border-radius: 6px; cursor: pointer; }
.table-tools button:hover { background: #f9fafb; }
</style>
"""
    script = """
<script>
function normalizeValue(v) {
  if (v === null || v === undefined) return '';
  return String(v).trim();
}

function csvEscape(v) {
  const s = normalizeValue(v);
  if (s.includes('\"') || s.includes(',') || s.includes('\\n')) {
    return '\"' + s.replace(/\"/g, '\"\"') + '\"';
  }
  return s;
}

function parseSortable(v) {
  const s = normalizeValue(v).replace(/,/g, '');
  if (s === '' || s.toLowerCase() === 'nan') return { type: 'text', value: '' };
  const n = Number(s);
  if (!Number.isNaN(n)) return { type: 'number', value: n };
  const d = Date.parse(s);
  if (!Number.isNaN(d)) return { type: 'date', value: d };
  return { type: 'text', value: s.toLowerCase() };
}

function applyFilters(table) {
  const tbody = table.querySelector('tbody');
  if (!tbody) return;
  const rows = Array.from(tbody.querySelectorAll('tr'));
  const filters = Array.from(table.querySelectorAll('thead tr.filter-row input')).map(i => i.value.toLowerCase().trim());

  rows.forEach(row => {
    const cells = Array.from(row.children);
    let visible = true;
    filters.forEach((f, idx) => {
      if (!f) return;
      // +1 offset because column 0 is the selection checkbox.
      const val = normalizeValue(cells[idx + 1] ? cells[idx + 1].innerText : '').toLowerCase();
      if (!val.includes(f)) visible = false;
    });
    row.style.display = visible ? '' : 'none';
  });
}

function sortTable(table, colIndex) {
  const tbody = table.querySelector('tbody');
  if (!tbody) return;
  const currentCol = table.dataset.sortCol ? Number(table.dataset.sortCol) : -1;
  const currentDir = table.dataset.sortDir || 'asc';
  const nextDir = (currentCol === colIndex && currentDir === 'asc') ? 'desc' : 'asc';
  table.dataset.sortCol = colIndex;
  table.dataset.sortDir = nextDir;

  const rows = Array.from(tbody.querySelectorAll('tr'));
  rows.sort((a, b) => {
    const av = parseSortable(a.children[colIndex] ? a.children[colIndex].innerText : '');
    const bv = parseSortable(b.children[colIndex] ? b.children[colIndex].innerText : '');
    let cmp = 0;
    if (av.type === bv.type) {
      if (av.value < bv.value) cmp = -1;
      else if (av.value > bv.value) cmp = 1;
    } else {
      const as = normalizeValue(a.children[colIndex] ? a.children[colIndex].innerText : '').toLowerCase();
      const bs = normalizeValue(b.children[colIndex] ? b.children[colIndex].innerText : '').toLowerCase();
      if (as < bs) cmp = -1;
      else if (as > bs) cmp = 1;
    }
    return nextDir === 'asc' ? cmp : -cmp;
  });

  rows.forEach(r => tbody.appendChild(r));

  const headers = table.querySelectorAll('thead tr:first-child th');
  headers.forEach((h, idx) => {
    const base = h.dataset.baseText || h.innerText.replace(/[▲▼]\\s*$/, '').trim();
    h.dataset.baseText = base;
    if (idx === colIndex) h.innerHTML = base + ' <span class=\"sort-ind\">' + (nextDir === 'asc' ? '▲' : '▼') + '</span>';
    else h.innerHTML = base;
  });
}

function makeTablesInteractive() {
  const tables = document.querySelectorAll('table.interactive');
  tables.forEach(table => {
    const thead = table.querySelector('thead');
    const headerRow = thead ? thead.querySelector('tr') : null;
    if (!thead || !headerRow) return;

    // Add selection column header.
    const selectTh = document.createElement('th');
    selectTh.innerText = 'Select';
    selectTh.style.cursor = 'default';
    headerRow.insertBefore(selectTh, headerRow.firstChild);

    // Add filter row
    const filterRow = document.createElement('tr');
    filterRow.className = 'filter-row';
    const emptyFilter = document.createElement('th');
    filterRow.appendChild(emptyFilter);
    Array.from(headerRow.children).forEach((th, colIdx) => {
      if (colIdx === 0) return;
      const fth = document.createElement('th');
      const input = document.createElement('input');
      input.type = 'text';
      input.placeholder = 'Filter';
      input.addEventListener('input', () => applyFilters(table));
      fth.appendChild(input);
      filterRow.appendChild(fth);

      th.addEventListener('click', () => sortTable(table, colIdx));
      th.title = 'Click to sort';
    });
    thead.appendChild(filterRow);

    // Add selection checkbox per row.
    const tbody = table.querySelector('tbody');
    if (!tbody) return;
    Array.from(tbody.querySelectorAll('tr')).forEach(row => {
      const td = document.createElement('td');
      const cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.className = 'row-select';
      cb.addEventListener('change', () => updateSelectionSummary(table));
      td.appendChild(cb);
      row.insertBefore(td, row.firstChild);
      row.addEventListener('click', (ev) => {
        if (ev.target && (ev.target.tagName === 'INPUT' || ev.target.tagName === 'A')) return;
        cb.checked = !cb.checked;
        updateSelectionSummary(table);
      });
    });

    // Add summary node after table.
    const summary = document.createElement('div');
    summary.className = 'selection-summary';
    summary.innerText = 'Selected rows: 0';
    table.parentNode.insertBefore(summary, table.nextSibling);
    updateSelectionSummary(table);
  });
}

function updateSelectionSummary(table) {
  const rows = Array.from(table.querySelectorAll('tbody tr'));
  const selected = rows.filter(r => {
    const cb = r.querySelector('input.row-select');
    return cb && cb.checked;
  });

  rows.forEach(r => r.classList.toggle('selected-row', selected.includes(r)));

  const headerCells = Array.from(table.querySelectorAll('thead tr:first-child th'))
    .map(h => (h.dataset.baseText || h.innerText || '').replace(/[▲▼]\\s*$/, '').trim());

  // Compute sums for numeric columns (excluding select column at index 0).
  const sums = {};
  for (let col = 1; col < headerCells.length; col++) {
    let hasNumeric = false;
    let s = 0;
    selected.forEach(r => {
      const cell = r.children[col];
      const parsed = parseSortable(cell ? cell.innerText : '');
      if (parsed.type === 'number') {
        hasNumeric = true;
        s += parsed.value;
      }
    });
    if (hasNumeric) sums[headerCells[col]] = s;
  }

  let summaryText = 'Selected rows: ' + selected.length;
  const parts = Object.keys(sums).map(k => '<code>' + k + ' sum: ' + sums[k].toFixed(2) + '</code>');
  if (parts.length > 0) summaryText += ' | ' + parts.join(' ');

  const summaryNode = table.nextElementSibling && table.nextElementSibling.classList.contains('selection-summary')
    ? table.nextElementSibling
    : null;
  if (summaryNode) summaryNode.innerHTML = summaryText;
}

function downloadTableCsv(tableId) {
  const table = document.getElementById(tableId);
  if (!table) return;

  const headerTh = Array.from(table.querySelectorAll('thead tr:first-child th'));
  // Exclude the leading "Select" column.
  const headers = headerTh.slice(1).map(th => (th.dataset.baseText || th.innerText || '').replace(/[▲▼]\\s*$/, '').trim());

  const rows = Array.from(table.querySelectorAll('tbody tr'))
    .filter(r => r.style.display !== 'none')
    .map(r => Array.from(r.children).slice(1).map(td => td.innerText));

  const lines = [];
  lines.push(headers.map(csvEscape).join(','));
  rows.forEach(r => lines.push(r.map(csvEscape).join(',')));

  const csv = lines.join('\\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  const ts = new Date().toISOString().replace(/[:T]/g, '-').slice(0, 16);
  a.href = url;
  a.download = tableId + '_' + ts + '.csv';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

window.addEventListener('DOMContentLoaded', makeTablesInteractive);
</script>
"""
    html = [
        "<html><head><meta charset='utf-8'><title>Saras Dashboard V1</title>",
        style,
        script,
        "</head><body>",
        "<h1>Saras Performance Dashboard V1</h1>",
        "<div class='meta'>Built from current available data (Dec 2025 - Feb 2026). "
        "This is a P0 working version with explicit data gaps.</div>",
        "<div class='toc'><a href='#tab1'>Tab 1: Executive Summary</a>"
        "<a href='#tab2'>Tab 2: Conversion</a>"
        "<a href='#tab3'>Tab 3: Demand Capture</a>"
        "<a href='#tab4'>Tab 4: Demand Generation</a>"
        "<a href='#tab5'>Tab 5: Reconciliation & Data Quality</a>"
        "<a href='#tab6'>Tab 6: Campaign Mapping</a></div>",
        "<div class='warn'><b>Known gaps:</b> MER cannot be computed until Saras total channel revenue arrives. "
        "Meta-Shopify Jan 2026 is pending validation/re-export. Instamart Dec 2025 is missing.</div>",
        "<h2 id='tab1'>Tab 1 - Executive Summary</h2>",
        _to_html_table(tab1_summary, "Brand Monthly Summary", "tab1_brand_monthly_summary"),
        _to_html_table(tab1_mix, "Channel Spend Mix (%)", "tab1_channel_spend_mix"),
        "<h2 id='tab2'>Tab 2 - Conversion Performance</h2>",
        _to_html_table(tab2, "Conversion Insights by Channel (Leadership View)", "tab2_conversion_insights"),
        _to_html_table(
            tab2_google_campaign,
            "Google Campaign-Level Conversion Insights (Double-Click)",
            "tab2_google_campaign_insights",
        ),
        _to_html_table(
            tab2_meta_campaign,
            "Meta Campaign-Level Conversion Insights (Double-Click)",
            "tab2_meta_campaign_insights",
        ),
        "<h2 id='tab3'>Tab 3 - Demand Capture</h2>",
        _to_html_table(tab3, "Demand Capture Channels", "tab3_demand_capture"),
        "<h2 id='tab4'>Tab 4 - Demand Generation</h2>",
        _to_html_table(tab4, "Demand Generation Metrics", "tab4_demand_generation"),
        "<h2 id='tab5'>Tab 5 - Reconciliation and Data Quality</h2>",
        _to_html_table(tab5_recon, "Attribution Reconciliation", "tab5_reconciliation"),
        _to_html_table(tab5_dq, "Data Quality Issues", "tab5_data_quality"),
        "<h2 id='tab6'>Tab 6 - Campaign to Brand Mapping</h2>",
        _to_html_table(tab6_map, "Campaign/Ad Set Mapping with Spend", "tab6_campaign_mapping"),
        "</body></html>",
    ]
    return "\n".join(html)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    exec_df = _read_csv("mart_exec_summary_monthly.csv")
    conv_df = _read_csv("mart_conversion_channel_monthly.csv")
    capture_df = _read_csv("mart_demand_capture_monthly.csv")
    gen_df = _read_csv("mart_demand_generation_monthly.csv")
    recon_df = _read_csv("mart_reconciliation_monthly.csv")
    dq_df = _read_csv("mart_data_quality_issues.csv")
    tab6_map = _build_tab6_campaign_brand_mapping()

    tab1_summary, tab1_mix = _build_tab1(exec_df, conv_df)
    tab2_insights = _build_tab2_conversion_insights(conv_df)
    tab2_google_campaign = _build_tab2_google_campaign_drilldown()
    tab2_meta_campaign = _build_tab2_meta_campaign_drilldown()
    tab3_view = _build_tab3_demand_capture_view(capture_df)
    tab4_view = _build_tab4_demand_generation_view(gen_df)

    tab1_summary.to_csv(OUT_DIR / "tab1_exec_summary.csv", index=False)
    tab1_mix.to_csv(OUT_DIR / "tab1_channel_spend_mix.csv", index=False)
    tab2_insights.to_csv(OUT_DIR / "tab2_conversion_performance.csv", index=False)
    tab2_google_campaign.to_csv(OUT_DIR / "tab2_google_campaign_insights.csv", index=False)
    tab2_meta_campaign.to_csv(OUT_DIR / "tab2_meta_campaign_insights.csv", index=False)
    tab3_view.to_csv(OUT_DIR / "tab3_demand_capture.csv", index=False)
    tab4_view.to_csv(OUT_DIR / "tab4_demand_generation.csv", index=False)
    recon_df.to_csv(OUT_DIR / "tab5_reconciliation.csv", index=False)
    dq_df.to_csv(OUT_DIR / "tab5_data_quality.csv", index=False)
    tab6_map.to_csv(OUT_DIR / "tab6_campaign_brand_mapping.csv", index=False)

    html = build_dashboard_html(
        tab1_summary=tab1_summary,
        tab1_mix=tab1_mix,
        tab2=tab2_insights,
        tab2_google_campaign=tab2_google_campaign,
        tab2_meta_campaign=tab2_meta_campaign,
        tab3=tab3_view,
        tab4=tab4_view,
        tab5_recon=recon_df,
        tab5_dq=dq_df,
        tab6_map=tab6_map,
    )
    (OUT_DIR / "dashboard_v1.html").write_text(html, encoding="utf-8")

    readme = """# Saras Dashboard V1

Generated from current marts.

Files:
- dashboard_v1.html (visual dashboard)
- tab1_exec_summary.csv
- tab1_channel_spend_mix.csv
- tab2_conversion_performance.csv
- tab2_google_campaign_insights.csv
- tab2_meta_campaign_insights.csv
- tab3_demand_capture.csv
- tab4_demand_generation.csv
- tab5_reconciliation.csv
- tab5_data_quality.csv
- tab6_campaign_brand_mapping.csv
"""
    (OUT_DIR / "README.md").write_text(readme, encoding="utf-8")

    print(f"Wrote dashboard package: {OUT_DIR}")


if __name__ == "__main__":
    main()
