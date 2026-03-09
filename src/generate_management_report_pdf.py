from __future__ import annotations

import textwrap
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
MARTS = ROOT / "data" / "derived" / "dashboard_v2" / "marts"
OUT_DIR = ROOT / "output" / "pdf"
REPORTS_DIR = ROOT / "data" / "derived" / "reports"

MONTHS = ["2025-12", "2026-01", "2026-02"]
MONTH_LABELS = {"2025-12": "Dec 2025", "2026-01": "Jan 2026", "2026-02": "Feb 2026"}


def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _fmt_currency(v: float | int | None) -> str:
    if v is None or pd.isna(v):
        return "-"
    return f"Rs {v:,.0f}"


def _fmt_pct(v: float | int | None) -> str:
    if v is None or pd.isna(v):
        return "-"
    return f"{v:.1f}%"


def _fmt_roas(v: float | int | None) -> str:
    if v is None or pd.isna(v):
        return "-"
    return f"{v:.2f}x"


def _iroas(d_rev: float, d_spend: float, min_abs_delta_spend: float = 50000) -> float | None:
    if pd.isna(d_rev) or pd.isna(d_spend) or abs(d_spend) < min_abs_delta_spend:
        return None
    return d_rev / d_spend


def _read_conv() -> pd.DataFrame:
    p = MARTS / "mart_conversion_channel_monthly.csv"
    df = pd.read_csv(p)
    df = df[df["month_start"].isin(MONTHS)].copy()
    for c in ["spend", "revenue", "orders"]:
        df[c] = _to_num(df[c])
    return df


def _read_gen() -> pd.DataFrame:
    p = MARTS / "mart_demand_generation_monthly.csv"
    df = pd.read_csv(p)
    df = df[df["month_start"].isin(MONTHS)].copy()
    df["metric_value"] = _to_num(df["metric_value"])
    return df


def _df_text(df: pd.DataFrame) -> list[str]:
    if df.empty:
        return ["(no rows)"]
    return df.to_string(index=False).splitlines()


def _wrap_lines(lines: list[str], width: int = 100) -> list[str]:
    out: list[str] = []
    for line in lines:
        if not line:
            out.append("")
            continue
        out.extend(textwrap.wrap(line, width=width) or [""])
    return out


def _paginate(lines: list[str], lines_per_page: int = 48) -> list[list[str]]:
    pages: list[list[str]] = []
    cur: list[str] = []
    for line in lines:
        cur.append(line)
        if len(cur) >= lines_per_page:
            pages.append(cur)
            cur = []
    if cur:
        pages.append(cur)
    return pages


def _pdf_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _write_text_pdf(pages: list[list[str]], out_path: Path) -> None:
    """
    Minimal PDF writer using built-in Helvetica font.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    objects: list[bytes] = []

    def add_object(data: bytes) -> int:
        objects.append(data)
        return len(objects)

    font_obj = add_object(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    page_obj_ids: list[int] = []
    content_obj_ids: list[int] = []

    # placeholder for pages object, fill after page objects exist
    pages_placeholder = add_object(b"<< >>")

    for lines in pages:
        content_lines = [
            "BT",
            "/F1 10 Tf",
            "14 TL",
            "50 800 Td",
        ]
        for line in lines:
            content_lines.append(f"({_pdf_escape(line)}) Tj")
            content_lines.append("T*")
        content_lines.append("ET")
        content_stream = "\n".join(content_lines).encode("latin-1", errors="replace")
        content_obj = add_object(
            b"<< /Length " + str(len(content_stream)).encode("ascii") + b" >>\nstream\n" + content_stream + b"\nendstream"
        )
        content_obj_ids.append(content_obj)

        page_obj = add_object(
            (
                "<< /Type /Page /Parent {PAGES} 0 R /MediaBox [0 0 595 842] "
                f"/Resources << /Font << /F1 {font_obj} 0 R >> >> "
                f"/Contents {content_obj} 0 R >>"
            ).encode("ascii")
        )
        page_obj_ids.append(page_obj)

    kids = " ".join([f"{pid} 0 R" for pid in page_obj_ids]).encode("ascii")
    pages_obj_data = (
        b"<< /Type /Pages /Kids [ " + kids + b" ] /Count " + str(len(page_obj_ids)).encode("ascii") + b" >>"
    )
    objects[pages_placeholder - 1] = pages_obj_data

    # Patch /Parent placeholders in page objects
    for i, pid in enumerate(page_obj_ids):
        objects[pid - 1] = objects[pid - 1].replace(b"{PAGES}", str(pages_placeholder).encode("ascii"))

    catalog_obj = add_object(f"<< /Type /Catalog /Pages {pages_placeholder} 0 R >>".encode("ascii"))

    # write file
    with out_path.open("wb") as f:
        f.write(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
        xref_positions = [0]
        for i, obj in enumerate(objects, start=1):
            xref_positions.append(f.tell())
            f.write(f"{i} 0 obj\n".encode("ascii"))
            f.write(obj)
            f.write(b"\nendobj\n")

        xref_start = f.tell()
        f.write(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
        f.write(b"0000000000 65535 f \n")
        for pos in xref_positions[1:]:
            f.write(f"{pos:010d} 00000 n \n".encode("ascii"))

        trailer = (
            f"trailer\n<< /Size {len(objects) + 1} /Root {catalog_obj} 0 R >>\n"
            f"startxref\n{xref_start}\n%%EOF\n"
        )
        f.write(trailer.encode("ascii"))


def _build_report() -> tuple[Path, Path]:
    conv = _read_conv()
    gen = _read_gen()

    platform = conv[conv["attribution_layer"] == "platform"].copy()
    budget = (
        platform.groupby(["month_start", "objective_class"], as_index=False)["spend"]
        .sum(min_count=1)
        .pivot(index="month_start", columns="objective_class", values="spend")
        .reindex(MONTHS)
        .fillna(0)
    )
    budget["total"] = budget.sum(axis=1)
    budget["capture_share"] = (budget.get("demand_capture", 0) / budget["total"]) * 100
    budget["generation_share"] = (budget.get("demand_generation", 0) / budget["total"]) * 100

    three_month_obj = (
        platform.groupby("objective_class", as_index=False)[["spend", "revenue", "orders"]]
        .sum(min_count=1)
        .fillna(0)
    )
    three_month_obj["roas"] = three_month_obj["revenue"] / three_month_obj["spend"]
    three_month_obj["spend_share"] = (three_month_obj["spend"] / three_month_obj["spend"].sum()) * 100

    cap_channels = platform[platform["objective_class"] == "demand_capture"].copy()
    cap_by_channel = (
        cap_channels.groupby("channel", as_index=False)[["spend", "revenue", "orders"]]
        .sum(min_count=1)
        .fillna(0)
        .sort_values("spend", ascending=False)
    )
    cap_by_channel["roas"] = cap_by_channel["revenue"] / cap_by_channel["spend"]
    cap_by_channel["cpa"] = cap_by_channel["spend"] / cap_by_channel["orders"]
    cap_by_channel["spend_share"] = (cap_by_channel["spend"] / cap_by_channel["spend"].sum()) * 100

    gen_meta = platform[platform["objective_class"] == "demand_generation"].copy()
    gen_month = (
        gen_meta.groupby("month_start", as_index=False)[["spend", "revenue", "orders"]]
        .sum(min_count=1)
        .sort_values("month_start")
    )
    gen_month["roas"] = gen_month["revenue"] / gen_month["spend"]

    gen_metrics = gen.pivot_table(
        index="month_start",
        columns=["channel", "metric_name"],
        values="metric_value",
        aggfunc="sum",
    ).reindex(MONTHS)

    obj_month = (
        platform.groupby(["objective_class", "month_start"], as_index=False)[["spend", "revenue"]]
        .sum(min_count=1)
        .sort_values(["objective_class", "month_start"])
    )
    iroas_rows: list[dict[str, object]] = []
    for objective, g in obj_month.groupby("objective_class"):
        g = g.sort_values("month_start").reset_index(drop=True)
        for i in range(1, len(g)):
            prev = g.iloc[i - 1]
            cur = g.iloc[i]
            d_spend = float(cur["spend"] - prev["spend"])
            d_rev = float(cur["revenue"] - prev["revenue"])
            iroas_rows.append(
                {
                    "objective_class": objective,
                    "period": f"{MONTH_LABELS[prev['month_start']]} -> {MONTH_LABELS[cur['month_start']]}",
                    "delta_spend": d_spend,
                    "delta_revenue": d_rev,
                    "iroas_proxy": _iroas(d_rev, d_spend),
                }
            )
    iroas_df = pd.DataFrame(iroas_rows)

    capture_total = three_month_obj.loc[
        three_month_obj["objective_class"] == "demand_capture", "spend"
    ].sum()
    generation_total = three_month_obj.loc[
        three_month_obj["objective_class"] == "demand_generation", "spend"
    ].sum()
    total_spend = float(capture_total + generation_total)
    capture_share_total = (capture_total / total_spend) * 100 if total_spend else 0
    generation_share_total = (generation_total / total_spend) * 100 if total_spend else 0

    roas_trend_text = ", ".join(
        [f"{MONTH_LABELS[r.month_start]} {_fmt_roas(r.roas)}" for r in gen_month.itertuples()]
    )

    lines: list[str] = []
    lines += [
        "PERFORMANCE MARKETING REPORT (DEC 2025 TO FEB 2026)",
        "",
        "EXECUTIVE SUMMARY",
        f"- Total paid spend: {_fmt_currency(total_spend)}",
        f"- Budget split: Demand Capture {_fmt_pct(capture_share_total)} | Demand Generation {_fmt_pct(generation_share_total)}",
        f"- Demand Capture weighted ROAS (platform): {_fmt_roas((cap_by_channel['revenue'].sum()/cap_by_channel['spend'].sum()))}",
        f"- Demand Generation weighted ROAS (platform): {_fmt_roas((gen_month['revenue'].sum()/gen_month['spend'].sum()))}",
        "",
        "1) BUDGET ALLOCATION BETWEEN DEMAND CAPTURE AND DEMAND GENERATION",
    ]
    b = budget[["demand_capture", "demand_generation", "capture_share", "generation_share"]].copy()
    b = b.reset_index()
    b["month_start"] = b["month_start"].map(MONTH_LABELS)
    lines += _df_text(b)
    lines += [
        "",
        "Interpretation:",
        "- Mix moved from generation-heavy in Dec to near 50:50 in Jan-Feb.",
        "- This indicates active reallocation toward capture over the quarter.",
        "",
        "2) WITHIN DEMAND CAPTURE - FINDINGS AND RECOMMENDATIONS",
    ]
    lines += _df_text(cap_by_channel)
    lines += [
        "",
        "Findings:",
        "- Capture spend concentrated in Google and Blinkit.",
        "- Instamart is smaller and lower-ROAS versus Google/Blinkit in current window.",
        "- Jan to Feb: total capture spend reduced while total capture revenue increased (efficiency improvement signal).",
        "",
        "Recommendations:",
        "- Keep capture near 50% of budget until weekly efficiency is stable.",
        "- Tighten Google search and PMax segmentation before scale.",
        "- Maintain CPA guardrails on Blinkit/Instamart and shift budget weekly toward best 2-week cohorts.",
        "",
        "3) WITHIN DEMAND GENERATION - FINDINGS AND RECOMMENDATIONS",
    ]
    lines += _df_text(gen_month)
    lines += [
        "",
        f"Meta demand-generation ROAS trend: {roas_trend_text}",
        "",
        "Demand-generation signal metrics:",
    ]
    gm_rows = []
    for month in MONTHS:
        gm_rows.append(
            {
                "month": MONTH_LABELS[month],
                "meta_impressions": gen_metrics.loc[month, ("meta", "impressions")] if ("meta", "impressions") in gen_metrics.columns else None,
                "meta_reach": gen_metrics.loc[month, ("meta", "reach")] if ("meta", "reach") in gen_metrics.columns else None,
                "meta_new_customers": gen_metrics.loc[month, ("meta", "new_customers")] if ("meta", "new_customers") in gen_metrics.columns else None,
                "d2c_new_customers": gen_metrics.loc[month, ("shopify_d2c", "d2c_new_customers")] if ("shopify_d2c", "d2c_new_customers") in gen_metrics.columns else None,
                "branded_search_volume": gen_metrics.loc[month, ("google", "branded_search_volume")] if ("google", "branded_search_volume") in gen_metrics.columns else None,
            }
        )
    lines += _df_text(pd.DataFrame(gm_rows))
    lines += [
        "",
        "Findings:",
        "- Meta remains primary generation engine; ROAS is strong but has cooled from Jan to Feb.",
        "- Reach and D2C new-customer trend softened into Feb.",
        "- Branded search has a missing Feb value in current source and should be backfilled before final inference.",
        "",
        "Recommendations:",
        "- Refresh creative every 2-3 weeks and preserve an 80:20 proven:test split.",
        "- Add a monthly incrementality test design (geo/cohort holdout).",
        "- Monitor new-customer and branded-search trend as joint health indicators.",
        "",
        "INCREMENTAL ROAS OBSERVATIONS (DIRECTIONAL)",
    ]
    lines += _df_text(iroas_df)
    lines += [
        "",
        "Note: iROAS proxy is Delta Revenue / Delta Spend across adjacent months. Treat as directional (short window, attribution noise).",
    ]

    wrapped_lines = _wrap_lines(lines, width=104)
    pages = _paginate(wrapped_lines, lines_per_page=49)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    md_path = REPORTS_DIR / "performance_marketing_report_dec2025_feb2026.md"
    md_path.write_text("\n".join(wrapped_lines), encoding="utf-8")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pdf_path = OUT_DIR / "performance_marketing_report_dec2025_feb2026.pdf"
    _write_text_pdf(pages, pdf_path)
    return pdf_path, md_path


def main() -> None:
    pdf_path, md_path = _build_report()
    print(f"Wrote report PDF: {pdf_path}")
    print(f"Wrote report summary: {md_path}")


if __name__ == "__main__":
    main()
