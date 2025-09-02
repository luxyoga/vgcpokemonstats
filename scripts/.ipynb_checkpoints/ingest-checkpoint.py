#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date

import duckdb
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta  # pip install python-dateutil
import string

# ============================
# Config
# ============================
# We will compose URLs like:
#   https://www.smogon.com/stats/{YYYY-MM}/chaos/{slug}
# where slug looks like:
#   gen9vgc2025regjbo3-1760.json
URL_TEMPLATE = "https://www.smogon.com/stats/{month}/chaos/{slug}"
RAW_DIR = Path("data/raw")  # downloaded files saved under data/raw/YYYY-MM/...

# ============================
# Transform helpers
# ============================
def _normalize_single_choice(d: Optional[Dict]) -> Dict[str, float]:
    if not isinstance(d, dict) or not d:
        return {}
    tot = sum(d.values())
    if tot <= 0:
        return {}
    return {k: (v / tot) for k, v in d.items()}

def _moves_absolute_pct(d: Optional[Dict]) -> Dict[str, float]:
    """
    Convert move counts to % of sets that include the move (absolute percentages).
    In chaos files, move counts sum to ~4 * (# sets).
    """
    if not isinstance(d, dict) or not d:
        return {}
    total = sum(d.values())
    if total <= 0:
        return {}
    set_count = total / 4.0
    if set_count == 0:
        return {}
    return {k: (v / set_count) for k, v in d.items()}

def _top_with_pct(d: Dict[str, float]) -> Tuple[Optional[str], Optional[float]]:
    if not d:
        return None, None
    k = max(d, key=d.get)
    return k, d[k]

def _top_n_with_pct(d: Dict[str, float], n: int = 4) -> List[Tuple[Optional[str], Optional[float]]]:
    if not d:
        return []
    return sorted(d.items(), key=lambda x: x[1], reverse=True)[:n]

def _parse_nature_from_spread(spread_key: Optional[str]) -> Optional[str]:
    if not isinstance(spread_key, str):
        return None
    return spread_key.split(":", 1)[0] if ":" in spread_key else None

def _strip_nature_from_spread(spread_key: Optional[str]) -> Optional[str]:
    if not isinstance(spread_key, str):
        return None
    return spread_key.split(":", 1)[1] if ":" in spread_key else spread_key

def parse_smogon_json_to_df(raw_json: dict, snapshot_month: str) -> pd.DataFrame:
    """
    Parse a Smogon chaos JSON (already loaded) into a tidy dataframe for one month.
    """
    # Chaos sometimes wraps content in {"data": {...}}; sometimes it's directly the dict
    container = raw_json.get("data", raw_json)
    rows = []

    for name, info in container.items():
        usage = info.get("usage") or info.get("Usage")
        raw_count = info.get("Raw count") or info.get("raw_count")

        abilities  = _normalize_single_choice(info.get("Abilities")  or {})
        items      = _normalize_single_choice(info.get("Items")      or {})
        spreads    = _normalize_single_choice(info.get("Spreads")    or {})
        tera_types = _normalize_single_choice(info.get("Tera Types") or {})
        moves_abs  = _moves_absolute_pct(info.get("Moves") or {})

        top_ability, _           = _top_with_pct(abilities)
        top_item, top_item_pct   = _top_with_pct(items)
        top_tera, top_tera_pct   = _top_with_pct(tera_types)
        top_spread, _            = _top_with_pct(spreads)

        top_moves = _top_n_with_pct(moves_abs, 4)
        top_moves += [(None, None)] * (4 - len(top_moves))  # pad to 4

        rows.append({
            "name": name,
            "snapshot_month": snapshot_month,
            "usage": usage,
            "raw_count": raw_count,
            "top_ability": top_ability,
            "top_item": top_item, "top_item_pct": top_item_pct,
            "top_tera_type": top_tera, "top_tera_pct": top_tera_pct,
            "top_spread": top_spread,
            "top_spread_no_nature": _strip_nature_from_spread(top_spread),
            "top_nature": _parse_nature_from_spread(top_spread),
            "move1": top_moves[0][0], "move1_pct": top_moves[0][1],
            "move2": top_moves[1][0], "move2_pct": top_moves[1][1],
            "move3": top_moves[2][0], "move3_pct": top_moves[2][1],
            "move4": top_moves[3][0], "move4_pct": top_moves[3][1],
        })

    return pd.DataFrame(rows)

# ============================
# DuckDB helpers (self-healing schema)
# ============================
def ensure_schema(con: duckdb.DuckDBPyConnection):
    # Create table if missing
    con.execute("""
    CREATE TABLE IF NOT EXISTS smogon_usage (
      name TEXT,
      snapshot_month TEXT,
      usage DOUBLE,
      raw_count DOUBLE,
      top_ability TEXT,
      top_item TEXT, top_item_pct DOUBLE,
      top_tera_type TEXT, top_tera_pct DOUBLE,
      top_spread TEXT,
      top_spread_no_nature TEXT,
      top_nature TEXT,
      move1 TEXT, move1_pct DOUBLE,
      move2 TEXT, move2_pct DOUBLE,
      move3 TEXT, move3_pct DOUBLE,
      move4 TEXT, move4_pct DOUBLE
    )
    """)
    # If snapshot_month was created with the wrong type in the past, fix by recreating
    info = con.execute("PRAGMA table_info('smogon_usage')").fetchdf()
    row = info[info["name"] == "snapshot_month"]
    if not row.empty and row.iloc[0]["type"].upper() not in ("TEXT", "VARCHAR"):
        con.execute("""
            CREATE TABLE smogon_usage__new AS
            SELECT
              name, CAST(snapshot_month AS TEXT) AS snapshot_month, usage, raw_count,
              top_ability, top_item, top_item_pct, top_tera_type, top_tera_pct,
              top_spread, top_spread_no_nature, top_nature,
              move1, move1_pct, move2, move2_pct, move3, move3_pct, move4, move4_pct
            FROM smogon_usage
        """)
        con.execute("DROP TABLE smogon_usage")
        con.execute("ALTER TABLE smogon_usage__new RENAME TO smogon_usage")

def upsert(con: duckdb.DuckDBPyConnection, df: pd.DataFrame):
    ensure_schema(con)
    df["snapshot_month"] = df["snapshot_month"].astype(str)

    con.register("smogon_df", df)

    # Overwrite each month we’re loading (idempotent per month)
    months = df["snapshot_month"].unique().tolist()
    for m in months:
        con.execute("DELETE FROM smogon_usage WHERE snapshot_month = ?", [m])

    # Insert explicitly by column name to avoid order mismatch
    con.execute("""
        INSERT INTO smogon_usage (
            name, snapshot_month, usage, raw_count, top_ability,
            top_item, top_item_pct, top_tera_type, top_tera_pct,
            top_spread, top_spread_no_nature, top_nature,
            move1, move1_pct, move2, move2_pct, move3, move3_pct, move4, move4_pct
        )
        SELECT
            name, snapshot_month, usage, raw_count, top_ability,
            top_item, top_item_pct, top_tera_type, top_tera_pct,
            top_spread, top_spread_no_nature, top_nature,
            move1, move1_pct, move2, move2_pct, move3, move3_pct, move4, move4_pct
        FROM smogon_df
    """)
    con.unregister("smogon_df")

def existing_months(con: duckdb.DuckDBPyConnection) -> List[str]:
    ensure_schema(con)
    return (
        con.execute("SELECT DISTINCT snapshot_month FROM smogon_usage ORDER BY 1").fetchdf()["snapshot_month"].tolist()
    )

# ============================
# Download helpers (latest reg, bo3, 1760)
# ============================
def candidate_reg_slugs(year: int) -> List[str]:
    """
    Generate candidate slugs for a given year, prioritizing:
      - latest regulation letter first (z..a)
      - bo3 only
      - 1760 ladder
    Example candidate: gen9vgc2025regjbo3-1760.json
    """
    regs = list(string.ascii_lowercase)[::-1]  # z..a
    out = []
    for r in regs:
        out.append(f"gen9vgc{year}reg{r}bo3-1760.json")
    return out

def probe_download(url_base: str, month: str, slugs: List[str]) -> Optional[Path]:
    """
    Try each slug until one returns 200. Save as data/raw/YYYY-MM/<slug>.
    """
    dest_dir = RAW_DIR / month
    dest_dir.mkdir(parents=True, exist_ok=True)
    for slug in slugs:
        url = f"{url_base}/{month}/chaos/{slug}"
        dest = dest_dir / slug
        try:
            r = requests.get(url, timeout=60)
            if r.status_code == 200 and r.content:
                dest.write_bytes(r.content)
                return dest
        except Exception:
            pass
    return None

def fetch_month_latest_bo3(month: str) -> Path:
    """
    Always download the latest regulation bo3 (1760) JSON for this month.
    """
    year = int(month.split("-")[0])
    slugs = candidate_reg_slugs(year)
    base = "https://www.smogon.com/stats"

    found = probe_download(base, month, slugs)
    if not found:
        # Provide a clear error with a preview of attempted slugs
        preview = ", ".join(slugs[:6]) + (" ..." if len(slugs) > 6 else "")
        raise FileNotFoundError(
            f"No bo3 1760 JSON found for {month}. Tried: {preview}"
        )
    return found

# ============================
# Month helpers
# ============================
def month_iter(start: str, end: str) -> List[str]:
    cur = datetime.strptime(start, "%Y-%m")
    stop = datetime.strptime(end, "%Y-%m")
    out: List[str] = []
    while cur <= stop:
        out.append(cur.strftime("%Y-%m"))
        cur += relativedelta(months=1)
    return out

def last_full_month_str() -> str:
    """Return YYYY-MM for the last fully completed month."""
    today = date.today().replace(day=1)
    last = today - relativedelta(months=1)
    return last.strftime("%Y-%m")

def sync_read_copy(src: str, dst: str):
    """Atomically refresh a read-only copy (so Streamlit can read without locks)."""
    tmp = dst + ".tmp"
    shutil.copy2(src, tmp)
    os.replace(tmp, dst)

# ============================
# CLI
# ============================
def main():
    ap = argparse.ArgumentParser(description="Ingest Smogon chaos JSONs into DuckDB (latest regulation, bo3, 1760).")
    ap.add_argument("--db", default="poke.duckdb", help="DuckDB file to write to.")

    # Manual modes
    ap.add_argument("--month", help="Single month YYYY-MM (e.g., 2025-07)")
    ap.add_argument("--from-month", dest="from_month", help="Start month YYYY-MM")
    ap.add_argument("--to-month", dest="to_month", help="End month YYYY-MM")

    # Auto mode (discover missing months)
    ap.add_argument("--auto", action="store_true", help="Discover and ingest missing months automatically")
    ap.add_argument("--start", help="Start month for auto mode (YYYY-MM). Example: 2025-01")
    ap.add_argument("--end", help="End month for auto mode (YYYY-MM). Default: last full month")
    ap.add_argument("--force", action="store_true", help="Reload months even if they already exist")

    # Utility flags
    ap.add_argument("--download-only", dest="download_only", action="store_true",
                    help="Only download JSON files; skip DB load")
    ap.add_argument("--sync-read-copy", dest="sync_read_copy", action="store_true",
                    help="After load, refresh a read-only copy (default path: poke_read.duckdb)")
    ap.add_argument("--read-copy-path", dest="read_copy_path", default="poke_read.duckdb",
                    help="Path of the read-only copy to refresh (used with --sync-read-copy)")

    args = ap.parse_args()

    # Decide which months to process
    months: List[str] = []

    if args.auto:
        start = args.start or "2025-01"     # earliest month of interest
        end = args.end or last_full_month_str()
        all_range = month_iter(start, end)

        con_tmp = duckdb.connect(args.db)
        have = set(existing_months(con_tmp))
        con_tmp.close()

        if args.force:
            months = all_range
        else:
            months = [m for m in all_range if m not in have]

        if not months:
            print(f"Nothing to do. All months {start}..{end} already present.")
            # still optionally sync read copy if requested
            if args.sync_read_copy and os.path.exists(args.db):
                try:
                    sync_read_copy(args.db, args.read_copy_path)
                    print(f"Synced read-only copy: {args.read_copy_path}")
                except Exception as e:
                    print(f"Warning: failed to sync read-only copy: {e}")
            return

        print(f"AUTO mode: will ingest {len(months)} month(s): {months[0]} .. {months[-1]}")

    elif args.month:
        months = [args.month]
    elif args.from_month and args.to_month:
        months = month_iter(args.from_month, args.to_month)
    else:
        ap.error("Provide --auto (optionally with --start/--end), or --month YYYY-MM, or --from-month/--to-month.")

    con = duckdb.connect(args.db)
    ok, fail = 0, 0

    for m in months:
        try:
            # 1) Download the month: latest regulation, bo3, 1760
            path = fetch_month_latest_bo3(m)

            if args.download_only:
                print(f"Downloaded {m} -> {path.name}")
                ok += 1
                continue

            # 2) Transform & upsert
            raw_json = json.loads(path.read_text())
            df = parse_smogon_json_to_df(raw_json, snapshot_month=m)
            if df.empty:
                print(f"[{m}] empty dataframe, skip")
                continue

            upsert(con, df)
            ok += 1
            print(f"[{m}] rows={len(df)}  OK")

        except Exception as e:
            fail += 1
            print(f"[{m}] ERROR: {e}")

    con.close()

    # 3) Optionally refresh a read-only copy for Streamlit
    if args.sync_read_copy and os.path.exists(args.db):
        try:
            sync_read_copy(args.db, args.read_copy_path)
            print(f"Synced read-only copy: {args.read_copy_path}")
        except Exception as e:
            print(f"Warning: failed to sync read-only copy: {e}")

    print(f"Done. ok={ok} fail={fail}")

if __name__ == "__main__":
    main()