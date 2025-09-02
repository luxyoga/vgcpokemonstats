import os
import re
from typing import List, Optional

import duckdb
import pandas as pd
import streamlit as st
from difflib import SequenceMatcher

# -----------------------------
# Page / CSS
# -----------------------------
st.set_page_config(page_title="Smogon Mini Dashboard", layout="wide")

st.markdown("""
<style>
.big-text{
  font-size: 1.6rem;
  line-height: 1.25;
  white-space: normal;
  word-break: break-word;
  overflow-wrap: anywhere;
  margin-top: 0.25rem;
}
.small-faint{
  color: rgba(255,255,255,0.6);
  font-size: 0.9rem;
}
</style>
""", unsafe_allow_html=True)

# -----------------------------
# DB connection (cached)
# -----------------------------
@st.cache_resource
def get_con() -> duckdb.DuckDBPyConnection:
    db_path = "poke_read.duckdb" if os.path.exists("poke_read.duckdb") else "poke.duckdb"
    con = duckdb.connect(db_path, read_only=True)
    return con

# -----------------------------
# Query helpers
# -----------------------------
@st.cache_data
def list_months() -> List[str]:
    df = get_con().execute("""
        SELECT DISTINCT snapshot_month
        FROM smogon_usage
        WHERE snapshot_month IS NOT NULL
        ORDER BY 1
    """).fetchdf()
    return df["snapshot_month"].tolist()

@st.cache_data
def list_names(month: str) -> List[str]:
    df = get_con().execute("""
        SELECT DISTINCT name
        FROM smogon_usage
        WHERE snapshot_month = ?
        ORDER BY 1
    """, [month]).fetchdf()
    return df["name"].tolist()

@st.cache_data
def list_names_all() -> List[str]:
    df = get_con().execute("""
        SELECT DISTINCT name
        FROM smogon_usage
        ORDER BY 1
    """).fetchdf()
    return df["name"].tolist()

def latest_month_for_exact_name(pokemon: str) -> Optional[str]:
    """Return the latest snapshot_month where this exact Pokémon name exists (uncached to avoid staleness)."""
    row = get_con().execute("""
        SELECT snapshot_month
        FROM smogon_usage
        WHERE LOWER(name) = LOWER(?)
        ORDER BY snapshot_month DESC
        LIMIT 1
    """, [pokemon]).fetchone()
    return row[0] if row else None

@st.cache_data
def top_n_table(month: str, n: int = 20) -> pd.DataFrame:
    return get_con().execute(f"""
        SELECT
          name,
          ROUND(CASE WHEN usage <= 1 THEN usage*100 ELSE usage END, 2) AS usage_pct,
          top_item,
          ROUND(COALESCE(top_item_pct,0)*100, 2) AS item_pct,
          top_tera_type,
          ROUND(COALESCE(top_tera_pct,0)*100, 2) AS tera_pct,
          top_spread_no_nature,
          top_nature,
          move1, ROUND(COALESCE(move1_pct,0)*100, 2) AS move1_pct,
          move2, ROUND(COALESCE(move2_pct,0)*100, 2) AS move2_pct,
          move3, ROUND(COALESCE(move3_pct,0)*100, 2) AS move3_pct,
          move4, ROUND(COALESCE(move4_pct,0)*100, 2) AS move4_pct
        FROM smogon_usage
        WHERE snapshot_month = ?
        ORDER BY usage DESC
        LIMIT {int(n)}
    """, [month]).fetchdf()

@st.cache_data
def top_n_table_alltime(n: int = 20) -> pd.DataFrame:
    """
    Aggregate across all months:
    - usage: average usage%
    - items/tera/spread/nature/moves: mode (most frequent)
    - *_pct columns: average % across months
    """
    return get_con().execute(f"""
        SELECT
          name,
          ROUND(AVG(CASE WHEN usage <= 1 THEN usage*100 ELSE usage END), 2) AS usage_pct,
          MODE(top_item) AS top_item,
          ROUND(AVG(COALESCE(top_item_pct,0)*100), 2) AS item_pct,
          MODE(top_tera_type) AS top_tera_type,
          ROUND(AVG(COALESCE(top_tera_pct,0)*100), 2) AS tera_pct,
          MODE(top_spread_no_nature) AS top_spread_no_nature,
          MODE(top_nature) AS top_nature,
          MODE(move1) AS move1, ROUND(AVG(COALESCE(move1_pct,0)*100), 2) AS move1_pct,
          MODE(move2) AS move2, ROUND(AVG(COALESCE(move2_pct,0)*100), 2) AS move2_pct,
          MODE(move3) AS move3, ROUND(AVG(COALESCE(move3_pct,0)*100), 2) AS move3_pct,
          MODE(move4) AS move4, ROUND(AVG(COALESCE(move4_pct,0)*100), 2) AS move4_pct
        FROM smogon_usage
        GROUP BY name
        ORDER BY usage_pct DESC
        LIMIT {int(n)}
    """).fetchdf()

@st.cache_data
def query_profile(pokemon: str, month: str) -> pd.DataFrame:
    return get_con().execute("""
        SELECT
          name,
          ROUND(CASE WHEN usage <= 1 THEN usage*100 ELSE usage END, 2) AS usage_pct,
          top_item, ROUND(COALESCE(top_item_pct,0)*100, 2) AS item_pct,
          top_tera_type, ROUND(COALESCE(top_tera_pct,0)*100, 2) AS tera_pct,
          top_spread_no_nature, top_nature,
          move1, ROUND(COALESCE(move1_pct,0)*100, 2) AS move1_pct,
          move2, ROUND(COALESCE(move2_pct,0)*100, 2) AS move2_pct,
          move3, ROUND(COALESCE(move3_pct,0)*100, 2) AS move3_pct,
          move4, ROUND(COALESCE(move4_pct,0)*100, 2) AS move4_pct
        FROM smogon_usage
        WHERE snapshot_month = ?
          AND LOWER(name) = LOWER(?)
        LIMIT 1
    """, [month, pokemon]).fetchdf()

@st.cache_data
def query_profile_all_time(pokemon: str) -> pd.DataFrame:
    return get_con().execute("""
        SELECT
          name,
          ROUND(AVG(CASE WHEN usage <= 1 THEN usage*100 ELSE usage END), 2) AS usage_pct,
          MODE(top_item) AS top_item,
          ROUND(AVG(COALESCE(top_item_pct,0)*100), 2) AS item_pct,
          MODE(top_tera_type) AS top_tera_type,
          ROUND(AVG(COALESCE(top_tera_pct,0)*100), 2) AS tera_pct,
          MODE(top_spread_no_nature) AS top_spread_no_nature,
          MODE(top_nature) AS top_nature,
          MODE(move1) AS move1, ROUND(AVG(COALESCE(move1_pct,0)*100), 2) AS move1_pct,
          MODE(move2) AS move2, ROUND(AVG(COALESCE(move2_pct,0)*100), 2) AS move2_pct,
          MODE(move3) AS move3, ROUND(AVG(COALESCE(move3_pct,0)*100), 2) AS move3_pct,
          MODE(move4) AS move4, ROUND(AVG(COALESCE(move4_pct,0)*100), 2) AS move4_pct
        FROM smogon_usage
        WHERE LOWER(name) = LOWER(?)
        GROUP BY name
        LIMIT 1
    """, [pokemon]).fetchdf()

# -----------------------------
# Matching helpers
# -----------------------------
def _norm(s: str) -> str:
    s = s.lower()
    s = s.replace("–", "-").replace("—", "-").replace("_", "-")
    # keep letters, numbers, space, and hyphen; everything else -> space
    s = re.sub(r"[^a-z0-9 -]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def best_name_candidates(query: str, names: List[str], k: int = 5, min_cutoff: float = 0.6) -> List[str]:
    if not query or not names:
        return []
    q = _norm(query)

    exact, sub, scored = [], [], []
    for n in names:
        nn = _norm(n)
        if nn == q:
            exact.append(n)
        elif q in nn or nn in q:
            sub.append((n, 0.95))
        else:
            s = SequenceMatcher(None, q, nn).ratio()
            scored.append((n, s))

    if exact:
        return exact[:k]

    sub.sort(key=lambda x: x[1], reverse=True)
    out = [n for (n, _) in sub][:k]

    scored.sort(key=lambda x: x[1], reverse=True)
    out += [n for (n, s) in scored if s >= min_cutoff]

    seen, final = set(), []
    for n in out:
        if n not in seen:
            final.append(n)
            seen.add(n)
        if len(final) >= k:
            break
    return final

# -----------------------------
# Meta distribution helpers
# -----------------------------
@st.cache_data
def meta_items(scope: str, month: Optional[str], top_k: int = 10) -> pd.DataFrame:
    if scope == "This month" and month and month != "All Time":
        return get_con().execute(f"""
            SELECT top_item AS label, COUNT(*) AS count
            FROM smogon_usage
            WHERE snapshot_month = ?
              AND top_item IS NOT NULL
            GROUP BY 1
            ORDER BY count DESC
            LIMIT {int(top_k)}
        """, [month]).fetchdf()
    else:
        return get_con().execute(f"""
            SELECT top_item AS label, COUNT(*) AS count
            FROM smogon_usage
            WHERE top_item IS NOT NULL
            GROUP BY 1
            ORDER BY count DESC
            LIMIT {int(top_k)}
        """).fetchdf()

@st.cache_data
def meta_teras(scope: str, month: Optional[str], top_k: int = 10) -> pd.DataFrame:
    if scope == "This month" and month and month != "All Time":
        return get_con().execute(f"""
            SELECT top_tera_type AS label, COUNT(*) AS count
            FROM smogon_usage
            WHERE snapshot_month = ?
              AND top_tera_type IS NOT NULL
            GROUP BY 1
            ORDER BY count DESC
            LIMIT {int(top_k)}
        """, [month]).fetchdf()
    else:
        return get_con().execute(f"""
            SELECT top_tera_type AS label, COUNT(*) AS count
            FROM smogon_usage
            WHERE top_tera_type IS NOT NULL
            GROUP BY 1
            ORDER BY count DESC
            LIMIT {int(top_k)}
        """).fetchdf()

@st.cache_data
def meta_pokemon_avg_usage(scope: str, month: Optional[str], top_k: int = 20) -> pd.DataFrame:
    if scope == "This month" and month and month != "All Time":
        return get_con().execute(f"""
            SELECT name AS label,
                   ROUND(CASE WHEN usage <= 1 THEN usage*100 ELSE usage END, 2) AS usage_pct
            FROM smogon_usage
            WHERE snapshot_month = ?
            ORDER BY usage_pct DESC
            LIMIT {int(top_k)}
        """, [month]).fetchdf()
    else:
        return get_con().execute(f"""
            SELECT name AS label,
                   ROUND(AVG(CASE WHEN usage <= 1 THEN usage*100 ELSE usage END), 2) AS usage_pct
            FROM smogon_usage
            GROUP BY name
            ORDER BY usage_pct DESC
            LIMIT {int(top_k)}
        """).fetchdf()
# -----------------------------
# UI
# -----------------------------
st.title("Smogon Mini Dashboard")

months = list_months()
months_plus = months + ["All Time"]  # add at bottom
selected_month = st.selectbox("Month", months_plus, index=len(months_plus)-1)

# Top area: two columns
left, right = st.columns([1.2, 1])

# -----------------------------
# LEFT: Top usage table
# -----------------------------
with left:
    topN = st.slider("Top N", min_value=5, max_value=100, value=20, step=1)   # up to 100

    if selected_month == "All Time":
        df_top = top_n_table_alltime(topN)
    else:
        df_top = top_n_table(selected_month, topN)

    df_show = df_top.rename(columns={
        "name": "name",
        "usage_pct": "Usage %",
        "top_item": "top_item",
        "item_pct": "Item %",
        "top_tera_type": "top_tera_type",
        "tera_pct": "Tera %",
        "top_spread_no_nature": "top_spread",
        "top_nature": "top_nature",
        "move1": "move1", "move1_pct": "move1_%", 
        "move2": "move2", "move2_pct": "move2_%", 
        "move3": "move3", "move3_pct": "move3_%", 
        "move4": "move4", "move4_pct": "move4_%"
    })
    st.dataframe(df_show, use_container_width=True, hide_index=True)

# -----------------------------
# RIGHT: Pokémon profile
# -----------------------------
with right:
    st.subheader("Pokémon profile")
    q_text = st.text_input("Search (partial ok, e.g., 'zacian')", value="")

    picked: Optional[str] = None
    matches_all: List[str] = []

    # candidates
    if q_text.strip():
        if selected_month == "All Time":
            matches_all = best_name_candidates(q_text, list_names_all(), k=5, min_cutoff=0.6)
        else:
            matches_month = best_name_candidates(q_text, list_names(selected_month), k=5, min_cutoff=0.6)
            matches_all = matches_month if matches_month else best_name_candidates(
                q_text, list_names_all(), k=5, min_cutoff=0.6
            )

        if matches_all:
            picked = st.selectbox("Matches", matches_all, index=0, key="profile_pick_name")
        else:
            st.info("No similar names found. Try another query.")
    else:
        if 'df_top' in locals() and not df_top.empty:
            picked = df_top.iloc[0]["name"]

    if picked:
        # ---- All-Time profile ----
        if selected_month == "All Time":
            prof = query_profile_all_time(picked)

            if prof.empty and matches_all:
                for alt_name in matches_all:
                    prof = query_profile_all_time(alt_name)
                    if not prof.empty:
                        picked = alt_name
                        break

            if prof.empty:
                st.info(f"No profile for **{picked}** across all months.")
            else:
                row = prof.iloc[0]
                st.subheader(picked)

                # Three-column layout with better spacing
                c1, c2, c3 = st.columns([1, 2, 2])
                with c1:
                    st.metric("Usage %", f"{row['usage_pct']:.2f}")
                with c2:
                    st.markdown("**Top item**")
                    st.markdown(f"{row['top_item']} ({row['item_pct']:.2f}%)")
                with c3:
                    st.markdown("**Top Tera**")
                    st.markdown(f"{row['top_tera_type']} ({row['tera_pct']:.2f}%)")

                st.markdown("---")
                st.markdown(
                    f"**Top spread:** {row['top_spread_no_nature']} | **Nature:** {row['top_nature']}"
                )
                st.markdown("<br>", unsafe_allow_html=True)

                # Moves in smaller font
                moves = []
                for i in range(1, 5):
                    mv, mv_pct = row.get(f"move{i}"), row.get(f"move{i}_pct")
                    if mv and not pd.isna(mv):
                        moves.append(f"{mv} ({mv_pct:.2f}%)")
                if moves:
                    st.markdown(
                        f"<p style='font-size: 0.9em'><b>Moves (% of sets):</b> {', '.join(moves)}</p>",
                        unsafe_allow_html=True
                    )

        # ---- Single-month profile with auto-switch ----
        else:
            prof = query_profile(picked, selected_month)
            switched_month = None
            switched_name = picked

            if prof.empty:
                latest = latest_month_for_exact_name(picked)
                if latest is not None:
                    switched_month = latest
                    prof = query_profile(picked, latest)

            if prof.empty and matches_all:
                for alt_name in matches_all:
                    if alt_name.lower() == picked.lower():
                        continue
                    latest_alt = latest_month_for_exact_name(alt_name)
                    if latest_alt is not None:
                        switched_name = alt_name
                        switched_month = latest_alt
                        prof = query_profile(alt_name, latest_alt)
                        if not prof.empty:
                            break

            if prof.empty:
                st.info(f"No profile for **{picked}** in {selected_month} or other months.")
            else:
                if switched_month and (switched_month != selected_month or switched_name != picked):
                    st.caption(
                        f"Showing **{switched_name}** from **{switched_month}** "
                        f"(not present in **{selected_month}**)."
                    )

                row = prof.iloc[0]
                st.subheader(row['name'])

                c1, c2, c3 = st.columns([1, 2, 2])
                with c1:
                    st.metric("Usage %", f"{row['usage_pct']:.2f}")
                with c2:
                    st.markdown("**Top item**")
                    st.markdown(f"{row['top_item']} ({row['item_pct']:.2f}%)")
                with c3:
                    st.markdown("**Top Tera**")
                    st.markdown(f"{row['top_tera_type']} ({row['tera_pct']:.2f}%)")

                st.markdown("---")
                st.markdown(
                    f"**Top spread:** {row['top_spread_no_nature']} | **Nature:** {row['top_nature']}"
                )
                st.markdown("<br>", unsafe_allow_html=True)

                moves = []
                for i in range(1, 5):
                    mv, mv_pct = row.get(f"move{i}"), row.get(f"move{i}_pct")
                    if mv and not pd.isna(mv):
                        moves.append(f"{mv} ({mv_pct:.2f}%)")
                if moves:
                    st.markdown(
                        f"<p style='font-size: 0.9em'><b>Moves (% of sets):</b> {', '.join(moves)}</p>",
                        unsafe_allow_html=True
                    )

# -----------------------------
# BOTTOM (full-width): Meta distributions
# -----------------------------
st.markdown("## Meta distributions")

scope = st.radio("Scope", ["This month", "All months"], horizontal=True, index=1)
chart_type = st.radio("Chart", ["Top Items", "Top Tera Types", "Pokémon (avg usage)"], horizontal=True, index=0)

if chart_type == "Top Items":
    items = meta_items(scope, selected_month if scope == "This month" else None, 12)
    if not items.empty:
        st.caption(f"Most common **TOP Item** across the {scope.lower()}")
        st.bar_chart(items.set_index("label"), use_container_width=True)
    else:
        st.caption("No item data.")
elif chart_type == "Top Tera Types":
    teras = meta_teras(scope, selected_month if scope == "This month" else None, 12)
    if not teras.empty:
        st.caption(f"Most common **TOP Tera Type** across the {scope.lower()}")
        st.bar_chart(teras.set_index("label"), use_container_width=True)
    else:
        st.caption("No tera data.")
else:
    poke = meta_pokemon_avg_usage(scope, selected_month if scope == "This month" else None, 20)
    if not poke.empty:
        st.caption(f"Top Pokémon by **average usage %** across the {scope.lower()}")
        st.bar_chart(poke.set_index("label"), use_container_width=True)
    else:
        st.caption("No Pokémon data.")