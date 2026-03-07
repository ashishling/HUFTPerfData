from __future__ import annotations

import datetime as dt
import json
import re
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "derived" / "analytics.duckdb"
SERVER_NAME = "huft-duckdb-mcp"
SERVER_VERSION = "0.1.0"
DEFAULT_PROTOCOL_VERSION = "2025-11-25"
MAX_ROWS_HARD_LIMIT = 5000
TRANSPORT_MODE = "content_length"
ASK_CACHE_TTL_SECONDS = 300
ASK_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}

WRITE_OR_RISKY_SQL_RE = re.compile(
    r"\b("
    r"insert|update|delete|drop|alter|create|replace|truncate|merge|grant|revoke|"
    r"attach|detach|copy|install|load|pragma|call|vacuum|export|import|"
    r"write_csv|read_csv_auto"
    r")\b",
    re.IGNORECASE,
)
READONLY_START_RE = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)
MONTH_YEAR_RE = re.compile(r"\b(20\d{2})[-/\s](0[1-9]|1[0-2])\b")


def _cache_get(key: str) -> dict[str, Any] | None:
    item = ASK_CACHE.get(key)
    if not item:
        return None
    ts, val = item
    if time.time() - ts > ASK_CACHE_TTL_SECONDS:
        ASK_CACHE.pop(key, None)
        return None
    return val


def _cache_set(key: str, value: dict[str, Any]) -> None:
    ASK_CACHE[key] = (time.time(), value)


def _log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return str(value)
    return value


def _format_tool_result(payload: dict[str, Any], *, is_error: bool = False) -> dict[str, Any]:
    return {
        "content": [{"type": "text", "text": json.dumps(payload, ensure_ascii=True, default=str)}],
        "structuredContent": payload,
        "isError": is_error,
    }


def _read_message() -> dict[str, Any] | None:
    global TRANSPORT_MODE
    first_line = sys.stdin.buffer.readline()
    if not first_line:
        return None

    stripped = first_line.lstrip()
    # Support newline-delimited JSON-RPC over stdio.
    if stripped.startswith(b"{"):
        TRANSPORT_MODE = "jsonl"
        msg = json.loads(first_line.decode("utf-8"))
        _log(f"[{SERVER_NAME}] recv method={msg.get('method')} id={msg.get('id')}")
        return msg

    # Support Content-Length framed JSON-RPC over stdio.
    raw_headers = bytearray(first_line)
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        raw_headers.extend(line)
        if line in (b"\r\n", b"\n"):
            break
        if len(raw_headers) > 64 * 1024:
            raise ValueError("Header too large")

    TRANSPORT_MODE = "content_length"
    header_text = raw_headers.decode("utf-8", errors="replace")
    headers: dict[str, str] = {}
    for line in header_text.replace("\r\n", "\n").split("\n"):
        if not line.strip():
            continue
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip().lower()] = v.strip()

    content_length_raw = headers.get("content-length") or headers.get("content_length")
    if not content_length_raw:
        return None
    content_length = int(content_length_raw)
    body = sys.stdin.buffer.read(content_length)
    if not body:
        return None
    msg = json.loads(body.decode("utf-8"))
    _log(f"[{SERVER_NAME}] recv method={msg.get('method')} id={msg.get('id')}")
    return msg


def _write_message(message: dict[str, Any]) -> None:
    body = json.dumps(message, ensure_ascii=True).encode("utf-8")
    if TRANSPORT_MODE == "jsonl":
        sys.stdout.buffer.write(body + b"\n")
    else:
        header = (
            f"Content-Length: {len(body)}\r\n"
            "Content-Type: application/json\r\n\r\n"
        ).encode("utf-8")
        sys.stdout.buffer.write(header)
        sys.stdout.buffer.write(body)
    sys.stdout.buffer.flush()
    _log(f"[{SERVER_NAME}] sent id={message.get('id')} keys={list(message.keys())}")


def _ok_response(msg_id: Any, result: Any) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": msg_id, "result": result}


def _error_response(msg_id: Any, code: int, message: str, data: Any | None = None) -> dict[str, Any]:
    err: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": "2.0", "id": msg_id, "error": err}


def _connect() -> duckdb.DuckDBPyConnection:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DuckDB file not found at {DB_PATH}")
    return duckdb.connect(str(DB_PATH), read_only=True)


def _list_datasets() -> list[dict[str, str]]:
    with _connect() as con:
        rows = con.execute(
            """
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name
            """
        ).fetchall()
    return [
        {
            "schema": row[0],
            "name": row[1],
            "kind": "view" if str(row[2]).upper() == "VIEW" else "table",
            "qualified_name": f"{row[0]}.{row[1]}",
        }
        for row in rows
    ]


def _resolve_dataset(name: str) -> tuple[str, str]:
    name = name.strip()
    if not name:
        raise ValueError("Dataset name cannot be empty.")

    with _connect() as con:
        if "." in name:
            schema, table = name.split(".", 1)
            rows = con.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE lower(table_schema) = lower(?) AND lower(table_name) = lower(?)
                """,
                [schema, table],
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                  AND lower(table_name) = lower(?)
                ORDER BY table_schema, table_name
                """,
                [name],
            ).fetchall()

    if not rows:
        raise ValueError(f"Dataset not found: {name}")
    if len(rows) > 1:
        options = [f"{r[0]}.{r[1]}" for r in rows]
        raise ValueError(f"Dataset name is ambiguous. Use one of: {options}")
    return rows[0][0], rows[0][1]


def _describe_dataset(name: str) -> dict[str, Any]:
    schema, table = _resolve_dataset(name)
    with _connect() as con:
        columns = con.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            [schema, table],
        ).fetchall()

        meta = con.execute(
            """
            SELECT table_type
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
            """,
            [schema, table],
        ).fetchone()

    return {
        "dataset": f"{schema}.{table}",
        "kind": "view" if str(meta[0]).upper() == "VIEW" else "table",
        "columns": [
            {"name": c[0], "type": c[1], "nullable": c[2] == "YES"}
            for c in columns
        ],
    }


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _sample_dataset(name: str, limit: int = 20) -> dict[str, Any]:
    schema, table = _resolve_dataset(name)
    limit = max(1, min(int(limit), 200))
    q = (
        f"SELECT * FROM {_quote_ident(schema)}.{_quote_ident(table)} "
        f"LIMIT {limit}"
    )
    with _connect() as con:
        cur = con.execute(q)
        col_names = [d[0] for d in cur.description]
        rows = cur.fetchall()

    recs = [
        {col_names[i]: _json_safe(v) for i, v in enumerate(row)}
        for row in rows
    ]
    return {
        "dataset": f"{schema}.{table}",
        "limit": limit,
        "row_count": len(recs),
        "rows": recs,
    }


def _sanitize_sql(sql: str) -> str:
    cleaned = sql.strip()
    if not cleaned:
        raise ValueError("SQL cannot be empty.")

    if cleaned.endswith(";"):
        cleaned = cleaned[:-1].strip()
    if ";" in cleaned:
        raise ValueError("Only one SQL statement is allowed.")
    if not READONLY_START_RE.match(cleaned):
        raise ValueError("Only SELECT/CTE queries are allowed.")
    if WRITE_OR_RISKY_SQL_RE.search(cleaned):
        raise ValueError("Blocked SQL contains disallowed keywords/functions.")
    return cleaned


def _query_readonly(sql: str, max_rows: int = 500) -> dict[str, Any]:
    sql = _sanitize_sql(sql)
    max_rows = max(1, min(int(max_rows), MAX_ROWS_HARD_LIMIT))
    wrapped = f"SELECT * FROM ({sql}) AS _q LIMIT {max_rows}"

    with _connect() as con:
        cur = con.execute(wrapped)
        col_names = [d[0] for d in cur.description]
        rows = cur.fetchall()

    recs = [
        {col_names[i]: _json_safe(v) for i, v in enumerate(row)}
        for row in rows
    ]
    return {"max_rows": max_rows, "row_count": len(recs), "rows": recs}


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _extract_month_start(question: str) -> str | None:
    q = question.lower()
    ym = MONTH_YEAR_RE.search(q)
    if ym:
        year, month = ym.group(1), ym.group(2)
        return f"{year}-{month}"

    # Keep to known available months in current dashboard marts.
    if ("dec" in q or "december" in q) and "2025" in q:
        return "2025-12"
    if ("jan" in q or "january" in q) and "2026" in q:
        return "2026-01"
    if ("feb" in q or "february" in q) and "2026" in q:
        return "2026-02"
    return None


def _build_ask_mart_sql(question: str, scope: str = "auto", top_n: int = 10) -> tuple[str, str, str | None]:
    q = question.lower()
    scope_n = (scope or "auto").strip().lower()
    month_start = _extract_month_start(q)
    n = max(1, min(int(top_n), 50))
    has_meta = "meta" in q or scope_n == "meta"
    has_google = "google" in q or scope_n == "google"
    has_campaign = "campaign" in q
    has_brand = "brand" in q
    asks_top = any(k in q for k in ("top", "highest", "largest", "best"))
    asks_total = any(k in q for k in ("total", "summary", "overall", "combined", "spend for jan", "spends for jan"))

    if has_brand:
        if month_start:
            raise ValueError(
                "Month-filtered brand spend is not available in current mart views. "
                "Try month-level channel spend or top campaigns."
            )
        if has_meta and not has_google:
            return (
                "SELECT brand, meta_spend_inr AS spend_inr "
                "FROM meta_spend_by_brand "
                "ORDER BY spend_inr DESC "
                f"LIMIT {n}",
                "brand_spend_meta",
                month_start,
            )
        if has_google and not has_meta:
            return (
                "SELECT brand, google_spend_inr AS spend_inr "
                "FROM google_spend_by_brand "
                "ORDER BY spend_inr DESC "
                f"LIMIT {n}",
                "brand_spend_google",
                month_start,
            )
        return (
            "SELECT brand, "
            "COALESCE(meta_spend_inr, 0) AS meta_spend_inr, "
            "COALESCE(google_spend_inr, 0) AS google_spend_inr, "
            "COALESCE(meta_spend_inr, 0) + COALESCE(google_spend_inr, 0) AS total_spend_inr "
            "FROM spend_by_brand "
            "ORDER BY total_spend_inr DESC "
            f"LIMIT {n}",
            "brand_spend_combined",
            month_start,
        )

    if has_campaign and asks_top:
        meta_campaign_col = "Ad set name" if month_start else "Campaign name"
        meta_filters = [
            f'"{meta_campaign_col}" IS NOT NULL',
            f'TRIM(CAST("{meta_campaign_col}" AS VARCHAR)) <> \'\'',
            f'TRIM(CAST("{meta_campaign_col}" AS VARCHAR)) <> \'--\'',
        ]
        google_filters = [
            '"Campaign" IS NOT NULL',
            'TRIM(CAST("Campaign" AS VARCHAR)) <> \'\'',
            'TRIM(CAST("Campaign" AS VARCHAR)) <> \'--\'',
        ]
        if month_start:
            month_filter = f"month_start = {_sql_literal(month_start)}"
            meta_filters.insert(0, month_filter)
            google_filters.insert(0, month_filter)

        meta_where_sql = " WHERE " + " AND ".join(meta_filters) + " "
        google_where_sql = " WHERE " + " AND ".join(google_filters) + " "

        if has_meta and not has_google:
            return (
                f'SELECT "{meta_campaign_col}" AS campaign, SUM("Amount spent (INR)") AS spend_inr '
                f"FROM {'meta_campaigns_monthly' if month_start else 'meta_campaigns'} "
                f"{meta_where_sql}"
                "GROUP BY 1 "
                "ORDER BY 2 DESC "
                f"LIMIT {n}",
                "top_campaigns_meta",
                month_start,
            )
        if has_google and not has_meta:
            return (
                'SELECT "Campaign" AS campaign, SUM("Cost") AS spend_inr '
                f"FROM {'google_campaigns_monthly' if month_start else 'google_campaigns'} "
                f"{google_where_sql}"
                "GROUP BY 1 "
                "ORDER BY 2 DESC "
                f"LIMIT {n}",
                "top_campaigns_google",
                month_start,
            )
        meta_src = 'meta_campaigns_monthly' if month_start else 'meta_campaigns'
        google_src = 'google_campaigns_monthly' if month_start else 'google_campaigns'
        return (
            "WITH u AS ("
            f'SELECT \'meta\' AS channel, "{meta_campaign_col}" AS campaign, SUM("Amount spent (INR)") AS spend_inr '
            f"FROM {meta_src}{meta_where_sql}GROUP BY 1,2 "
            "UNION ALL "
            'SELECT \'google\' AS channel, "Campaign" AS campaign, SUM("Cost") AS spend_inr '
            f"FROM {google_src}{google_where_sql}GROUP BY 1,2"
            ") "
            "SELECT channel, campaign, spend_inr "
            "FROM u "
            "ORDER BY spend_inr DESC "
            f"LIMIT {n}",
            "top_campaigns_combined",
            month_start,
        )

    if asks_total or "spend" in q or "cost" in q:
        if month_start:
            month_lit = _sql_literal(month_start)
            if has_meta and not has_google:
                return (
                    "SELECT month_start, meta_spend_inr AS spend_inr "
                    "FROM monthly_paid_spend_meta_google "
                    f"WHERE month_start = {month_lit}",
                    "total_spend_meta_month",
                    month_start,
                )
            if has_google and not has_meta:
                return (
                    "SELECT month_start, google_spend_inr AS spend_inr "
                    "FROM monthly_paid_spend_meta_google "
                    f"WHERE month_start = {month_lit}",
                    "total_spend_google_month",
                    month_start,
                )
            return (
                "SELECT month_start, meta_spend_inr, google_spend_inr, total_spend_inr "
                "FROM monthly_paid_spend_meta_google "
                f"WHERE month_start = {month_lit}",
                "total_spend_combined_month",
                month_start,
            )
        if has_meta and not has_google:
            return (
                'SELECT \'meta\' AS channel, SUM("Amount spent (INR)") AS spend_inr, COUNT(*) AS campaign_count '
                "FROM meta_campaigns",
                "total_spend_meta",
                month_start,
            )
        if has_google and not has_meta:
            return (
                'SELECT \'google\' AS channel, SUM("Cost") AS spend_inr, COUNT(*) AS campaign_count '
                "FROM google_campaigns",
                "total_spend_google",
                month_start,
            )
        return (
            "WITH totals AS ("
            'SELECT \'meta\' AS channel, SUM("Amount spent (INR)") AS spend_inr, COUNT(*) AS campaign_count '
            "FROM meta_campaigns "
            "UNION ALL "
            'SELECT \'google\' AS channel, SUM("Cost") AS spend_inr, COUNT(*) AS campaign_count '
            "FROM google_campaigns"
            ") "
            "SELECT * FROM totals "
            "UNION ALL "
            "SELECT 'combined' AS channel, SUM(spend_inr) AS spend_inr, SUM(campaign_count) AS campaign_count "
            "FROM totals",
            "total_spend_combined",
            month_start,
        )

    raise ValueError(
        "Unable to infer SQL from question. Try asking for total spend, top campaigns, or brand spend."
    )


def _ask_mart(question: str, scope: str = "auto", max_rows: int = 200, top_n: int = 10) -> dict[str, Any]:
    q = (question or "").strip()
    if not q:
        raise ValueError("question is required.")
    max_rows = max(1, min(int(max_rows), MAX_ROWS_HARD_LIMIT))

    cache_key = f"{q}|{scope}|{max_rows}|{top_n}"
    cached = _cache_get(cache_key)
    if cached is not None:
        out = dict(cached)
        out["cached"] = True
        return out

    sql, intent, month_start = _build_ask_mart_sql(q, scope, top_n)
    result = _query_readonly(sql, max_rows=max_rows)
    payload = {
        "question": q,
        "scope": scope,
        "intent": intent,
        "month_start": month_start,
        "generated_sql": sql,
        "cached": False,
        "result": result,
        "notes": [
            "Data is monthly aggregate (no daily granularity).",
            "Month parsing currently supports Dec 2025, Jan 2026, and Feb 2026.",
        ],
    }
    _cache_set(cache_key, payload)
    return payload


def _tool_list() -> dict[str, Any]:
    return {
        "tools": [
            {
                "name": "list_datasets",
                "description": (
                    "List available DuckDB tables/views. Data is monthly aggregate marketing data."
                ),
                "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
            },
            {
                "name": "describe_dataset",
                "description": "Describe schema (columns/types) for a table or view.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Dataset name, optionally schema-qualified."}
                    },
                    "required": ["name"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "sample_dataset",
                "description": "Return up to 200 sample rows from a dataset.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 20},
                    },
                    "required": ["name"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "query_readonly",
                "description": (
                    "Run read-only SQL against DuckDB. Only SELECT/CTE allowed; "
                    "single statement; hard row cap 5000."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "sql": {"type": "string"},
                        "max_rows": {"type": "integer", "minimum": 1, "maximum": MAX_ROWS_HARD_LIMIT, "default": 500},
                    },
                    "required": ["sql"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "ask_mart",
                "description": (
                    "Ask a natural-language analytics question; server generates safe SQL and returns results."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "question": {"type": "string"},
                        "scope": {
                            "type": "string",
                            "enum": ["auto", "meta", "google", "brand"],
                            "default": "auto",
                        },
                        "top_n": {"type": "integer", "minimum": 1, "maximum": 50, "default": 10},
                        "max_rows": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": MAX_ROWS_HARD_LIMIT,
                            "default": 200,
                        },
                    },
                    "required": ["question"],
                    "additionalProperties": False,
                },
            },
        ]
    }


def _handle_tools_call(params: dict[str, Any]) -> dict[str, Any]:
    name = params.get("name")
    arguments = params.get("arguments") or {}

    try:
        if name == "list_datasets":
            return _format_tool_result({"datasets": _list_datasets()})
        if name == "describe_dataset":
            return _format_tool_result(_describe_dataset(arguments["name"]))
        if name == "sample_dataset":
            return _format_tool_result(_sample_dataset(arguments["name"], arguments.get("limit", 20)))
        if name == "query_readonly":
            return _format_tool_result(_query_readonly(arguments["sql"], arguments.get("max_rows", 500)))
        if name == "ask_mart":
            return _format_tool_result(
                _ask_mart(
                    arguments["question"],
                    arguments.get("scope", "auto"),
                    arguments.get("max_rows", 200),
                    arguments.get("top_n", 10),
                )
            )
        return _format_tool_result({"error": f"Unknown tool: {name}"}, is_error=True)
    except Exception as exc:  # noqa: BLE001
        return _format_tool_result({"error": str(exc)}, is_error=True)


def _handle_request(message: dict[str, Any]) -> dict[str, Any] | None:
    method = message.get("method")
    params = message.get("params", {})
    msg_id = message.get("id")
    is_notification = msg_id is None

    if method == "notifications/initialized":
        return None

    if method == "initialize":
        client_version = (params or {}).get("protocolVersion")
        protocol_version = client_version if isinstance(client_version, str) and client_version else DEFAULT_PROTOCOL_VERSION
        return _ok_response(
            msg_id,
            {
                "protocolVersion": protocol_version,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
            },
        )
    if method == "ping":
        return _ok_response(msg_id, {})
    if method == "tools/list":
        return _ok_response(msg_id, _tool_list())
    if method == "tools/call":
        return _ok_response(msg_id, _handle_tools_call(params))

    if is_notification:
        return None
    return _error_response(msg_id, -32601, f"Method not found: {method}")


def main() -> int:
    _log(f"[{SERVER_NAME}] starting, db={DB_PATH}")
    while True:
        try:
            msg = _read_message()
            if msg is None:
                return 0
            response = _handle_request(msg)
            if response is not None:
                _write_message(response)
        except Exception as exc:  # noqa: BLE001
            _log(f"[{SERVER_NAME}] fatal error: {exc}")
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
