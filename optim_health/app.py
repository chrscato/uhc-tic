# app.py
# Streamlit dashboard to compare UHC rates vs GA WC + Medicare benchmarks
# Run: streamlit run app.py

import io
import numpy as np
import pandas as pd
import streamlit as st

# Optional: Altair for real histograms (fallback to st.bar_chart if missing)
try:
    import altair as alt
    ALT_AVAILABLE = True
except Exception:
    ALT_AVAILABLE = False

st.set_page_config(page_title="UHC vs Benchmarks", layout="wide")

# ------------------------- Helpers -------------------------

def norm_code_series(s: pd.Series) -> pd.Series:
    """Normalize billing codes (strip spaces/dots, upper-case for HCPCS/CPT)."""
    if s is None or s.empty:
        return s
    s = s.astype(str).str.strip()
    # remove spaces/dots commonly found in CPT/HCPCS (e.g., "27 132" or "J-1100")
    s = s.str.replace(r"[\s\.\-]+", "", regex=True).str.upper()
    return s

@st.cache_data(show_spinner=False)
def load_data(file_or_path) -> pd.DataFrame:
    if file_or_path is None:
        return pd.DataFrame()
    if hasattr(file_or_path, "read"):
        # Check file extension for uploaded file
        name = getattr(file_or_path, "name", "").lower()
        if name.endswith(".parquet"):
            return pd.read_parquet(file_or_path)
        return pd.read_csv(file_or_path)
    
    # Check file extension for path
    if str(file_or_path).lower().endswith(".parquet"):
        return pd.read_parquet(file_or_path)
    return pd.read_csv(file_or_path)

def ensure_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce numerics, compute uhc_rate fallback, robust % metrics (no inf)."""
    if df is None or df.empty:
        return df.copy()

    df = df.copy()

    # pick a code column, normalize to billing_code
    def pick_first_column(candidates):
        for c in candidates:
            if c in df.columns:
                return c
        return None

    code_col = pick_first_column(["billing_code", "code", "code_norm"])
    if code_col is not None and "billing_code" not in df.columns:
        df["billing_code"] = df[code_col]

    if "billing_code" in df.columns:
        df["billing_code"] = norm_code_series(df["billing_code"])

    # coerce numerics for common rate columns
    for c in ["uhc_rate", "ga_wc_rate", "medicare_allowed", "negotiated_rate", "price"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # uhc_rate fallback
    if "uhc_rate" not in df.columns or df["uhc_rate"].isna().all():
        candidates = [c for c in ["uhc_rate", "negotiated_rate", "price", "uhc_rate_median"] if c in df.columns]
        if candidates:
            df["uhc_rate"] = pd.to_numeric(df[candidates].bfill(axis=1).iloc[:, 0], errors="coerce")
        else:
            df["uhc_rate"] = np.nan

    # robust % of GA WC
    if "ga_wc_rate" in df.columns:
        df["pct_of_ga_wc"] = np.where(df["ga_wc_rate"] > 0, df["uhc_rate"] / df["ga_wc_rate"], np.nan)

    # robust % of Medicare
    if "medicare_allowed" in df.columns:
        df["pct_of_medicare"] = np.where(df["medicare_allowed"] > 0, df["uhc_rate"] / df["medicare_allowed"], np.nan)

    # remove infinities if any slipped in
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    return df

def audit(label: str, df: pd.DataFrame):
    st.caption(f"**{label}:** {len(df):,} rows")

def pct_slider_bounds(series: pd.Series, default_max=3.0):
    """Return slider min/max and default selection (clip extremes)."""
    if series is None or series.dropna().empty:
        return 0.0, default_max, (0.0, default_max)
    s = series.dropna()
    lo = max(0.0, float(np.nanquantile(s, 0.01)))
    hi = float(np.nanquantile(s, 0.99))
    hi = min(default_max, max(0.2, hi))  # clamp to a practical top
    return 0.0, hi, (0.0, hi)

def rate_slider_bounds(series: pd.Series):
    if series is None or series.dropna().empty:
        return 0.0, 5000.0, (0.0, 5000.0)
    s = series.dropna()
    lo = float(np.nanquantile(s, 0.01))
    hi = float(np.nanquantile(s, 0.99))
    lo = max(0.0, lo)
    hi = max(lo + 1.0, hi)
    return lo, hi, (lo, hi)

def hist_chart(series: pd.Series, title: str, max_x: float = 3.0):
    s = series.dropna()
    if s.empty:
        st.info(f"No data for {title}.")
        return
    s = s.clip(0, max_x)
    if ALT_AVAILABLE:
        bins = np.linspace(0, max_x, 31)
        hist, edges = np.histogram(s, bins=bins)
        data = pd.DataFrame({"bin_left": edges[:-1], "count": hist})
        chart = alt.Chart(data).mark_bar().encode(
            x=alt.X("bin_left:Q", bin=alt.Bin(step=max_x/30), title=title),
            y=alt.Y("count:Q", title="Count"),
            tooltip=["bin_left", "count"]
        ).properties(height=220)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.write(f"Altair not available; showing first 30 values for {title}.")
        st.bar_chart(s.head(30))

def fmt_pct(x):
    return f"{x*100:.1f}%" if pd.notna(x) else "—"

# ------------------------- Data load -------------------------

with st.sidebar:
    st.header("Data")
    uploaded = st.file_uploader("Upload data file", type=["csv", "parquet"])
    st.caption("If no file is uploaded, the app will try to open `combined_df.csv` from the current folder.")

df = load_data(uploaded) if uploaded is not None else load_data("combined_df.csv")

if df.empty:
    st.error("No data loaded. Upload a CSV or place `combined_df.csv` in the working directory.")
    st.stop()

df = ensure_metrics(df)

# ------------------------- Sidebar Filters -------------------------

with st.sidebar:
    st.header("Filters")

    # Categorical filters (graceful if columns absent)
    def multiselect_if(col, label=None):
        if col in df.columns:
            values = sorted(df[col].dropna().astype(str).unique().tolist())
            default = values  # select all by default
            sel = st.multiselect(label or col, values, default=default)
            return col, set(sel)
        return None, None

    col_bc, sel_bc = multiselect_if("billing_class", "Billing class")
    col_type, sel_type = multiselect_if("billing_code_type", "Code type")
    col_cbsa, sel_cbsa = multiselect_if("CBSA Title", "CBSA")

    # Optional bool-like flags if present
    include_ortho = True
    if "ortho_bucket" in df.columns:
        # Allow user to filter to ortho or not
        ortho_state = st.selectbox("Ortho bucket", ["All", "Ortho only", "Non-ortho"])
        include_ortho = ortho_state

    # Text search
    st.markdown("**Search**")
    code_q = st.text_input("Billing code contains")
    desc_q = st.text_input("Description contains")

    # Sliders for % of GA WC and % of Medicare
    st.markdown("**Percent ranges**")
    ga_min, ga_max, ga_default = pct_slider_bounds(df.get("pct_of_ga_wc"))
    pct_ga_min, pct_ga_max = st.slider("UHC as % of GA WC", min_value=float(ga_min), max_value=float(ga_max),
                                       value=(float(ga_default[0]), float(ga_default[1])))
    include_missing_ga = st.checkbox("Include rows with missing GA WC", value=True)

    med_min, med_max, med_default = pct_slider_bounds(df.get("pct_of_medicare"))
    pct_med_min, pct_med_max = st.slider("UHC as % of Medicare", min_value=float(med_min), max_value=float(med_max),
                                         value=(float(med_default[0]), float(med_default[1])))

    # UHC rate slider
    st.markdown("**UHC rate ($)**")
    rmin, rmax, rdefault = rate_slider_bounds(df.get("uhc_rate"))
    rate_min, rate_max = st.slider("Dollars", min_value=float(rmin), max_value=float(rmax),
                                   value=(float(rdefault[0]), float(rdefault[1])))

# ------------------------- Apply Filters w/ Audit -------------------------

f = df.copy()
audit("Loaded", f)

# Categorical
if col_bc and sel_bc:
    f = f[f[col_bc].astype(str).isin(sel_bc)]
if col_type and sel_type:
    f = f[f[col_type].astype(str).isin(sel_type)]
if col_cbsa and sel_cbsa:
    f = f[f[col_cbsa].astype(str).isin(sel_cbsa)]

# Ortho toggle
if include_ortho != True and "ortho_bucket" in f.columns:
    if include_ortho == "Ortho only":
        f = f[f["ortho_bucket"].astype(str).str.lower().isin(["1", "true", "yes", "ortho"])]
    elif include_ortho == "Non-ortho":
        f = f[~f["ortho_bucket"].astype(str).str.lower().isin(["1", "true", "yes", "ortho"])]

# Search text
if code_q:
    f = f[f["billing_code"].astype(str).str.contains(code_q.strip(), case=False, na=False)] if "billing_code" in f.columns else f
if desc_q and "description" in f.columns:
    f = f[f["description"].astype(str).str.contains(desc_q.strip(), case=False, na=False)]

audit("After categorical + search", f)

# Percent filters
if "pct_of_ga_wc" in f.columns:
    ga_mask = f["pct_of_ga_wc"].between(pct_ga_min, pct_ga_max)
    if include_missing_ga:
        ga_mask = ga_mask | f["pct_of_ga_wc"].isna()
    f = f[ga_mask]

if "pct_of_medicare" in f.columns:
    med_mask = f["pct_of_medicare"].between(pct_med_min, pct_med_max) | f["pct_of_medicare"].isna()
    f = f[med_mask]

audit("After % filters", f)

# UHC rate filter
if "uhc_rate" in f.columns:
    rate_mask = f["uhc_rate"].between(rate_min, rate_max) | f["uhc_rate"].isna()
    f = f[rate_mask]

audit("After UHC rate ($)", f)

if f.empty:
    st.warning("No rows match the current filters. Tip: widen the sliders or include rows with missing GA WC.")
    st.stop()

# ------------------------- Header + Explain -------------------------

st.title("UHC Rates vs. Benchmarks")
st.caption("Explore how UHC in-network rates compare to **GA Workers' Comp** fee schedule and **Medicare** allowed amounts.")

with st.expander("How are the percentages computed?"):
    st.markdown("""
- **% of GA WC** = `UHC rate ÷ GA WC fee schedule`.  
  If GA WC is unavailable or 0, it is shown as **missing**.
- **% of Medicare** = `UHC rate ÷ Medicare allowed`.  
- All divisions guard against divide-by-zero to avoid infinite values.
    """)

# ------------------------- KPI Cards -------------------------

total_rows = len(f)
ga_present = f["ga_wc_rate"].gt(0).sum() if "ga_wc_rate" in f.columns else 0
med_present = f["medicare_allowed"].gt(0).sum() if "medicare_allowed" in f.columns else 0
pct_ga_present = ga_present / total_rows if total_rows else np.nan
pct_med_present = med_present / total_rows if total_rows else np.nan

median_uhc = float(f["uhc_rate"].median()) if "uhc_rate" in f.columns else np.nan
median_pct_ga = float(f["pct_of_ga_wc"].median()) if "pct_of_ga_wc" in f.columns else np.nan
median_pct_med = float(f["pct_of_medicare"].median()) if "pct_of_medicare" in f.columns else np.nan

c1, c2, c3, c4 = st.columns(4)
c1.metric("Rows (after filters)", f"{total_rows:,}")
c2.metric("Median UHC rate", f"${median_uhc:,.2f}" if pd.notna(median_uhc) else "—")
c3.metric("% rows with GA WC", fmt_pct(pct_ga_present) if pd.notna(pct_ga_present) else "—")
c4.metric("Median % of Medicare", f"{median_pct_med*100:.1f}%" if pd.notna(median_pct_med) else "—")

# ------------------------- Charts -------------------------

ch1, ch2 = st.columns(2)
with ch1:
    if "pct_of_ga_wc" in f.columns and f["pct_of_ga_wc"].notna().any():
        hist_chart(f["pct_of_ga_wc"], "UHC as % of GA WC (0–300%)", max_x=3.0)
    else:
        st.info("No GA WC % available in current selection.")
with ch2:
    if "pct_of_medicare" in f.columns and f["pct_of_medicare"].notna().any():
        hist_chart(f["pct_of_medicare"], "UHC as % of Medicare (0–300%)", max_x=3.0)
    else:
        st.info("No Medicare % available in current selection.")

# ------------------------- Views: Row-level vs Code Summary -------------------------

view = st.radio("View", ["Row-level detail", "Code-level summary"], horizontal=True)

if view == "Code-level summary":
    # Aggregate by billing_code (+ description if present), median of rates
    group_cols = [c for c in ["billing_code", "description"] if c in f.columns]
    if not group_cols:
        st.info("No billing_code column to summarize by; showing row-level instead.")
    else:
        g = (f
             .groupby(group_cols, dropna=False)
             .agg(uhc_rate_med=("uhc_rate", "median"),
                  ga_wc_med=("ga_wc_rate", "median") if "ga_wc_rate" in f.columns else ("uhc_rate","size"),
                  medicare_med=("medicare_allowed", "median") if "medicare_allowed" in f.columns else ("uhc_rate","size"),
                  rows=("uhc_rate", "size"))
             .reset_index())
        # Recompute %s on medians
        if "ga_wc_med" in g.columns:
            g["pct_of_ga_wc_med"] = np.where(g["ga_wc_med"] > 0, g["uhc_rate_med"] / g["ga_wc_med"], np.nan)
        if "medicare_med" in g.columns:
            g["pct_of_medicare_med"] = np.where(g["medicare_med"] > 0, g["uhc_rate_med"] / g["medicare_med"], np.nan)

        core_cols = [c for c in [
            "billing_code", "description",
            "uhc_rate_med", "ga_wc_med", "pct_of_ga_wc_med",
            "medicare_med", "pct_of_medicare_med", "rows"
        ] if c in g.columns]

        st.subheader("Code-level summary (medians)")
        st.dataframe(g[core_cols].sort_values("uhc_rate_med", ascending=False), use_container_width=True)

        # Download aggregated
        buf = io.StringIO()
        g.to_csv(buf, index=False)
        st.download_button("Download code summary (CSV)", data=buf.getvalue(), file_name="code_summary.csv", mime="text/csv")
else:
    # Row-level core table
    core_cols = [c for c in [
        "billing_code", "description", "billing_class", "CBSA Title",
        "uhc_rate", "ga_wc_rate", "pct_of_ga_wc",
        "medicare_allowed", "pct_of_medicare",
        "reporting_entity_name", "last_updated_on_x", "version_x"
    ] if c in f.columns]

    st.subheader("Results (core columns)")
    st.dataframe(f[core_cols].sort_values(["billing_code","uhc_rate"], ascending=[True, False]), use_container_width=True)

    with st.expander("Show all columns"):
        st.dataframe(f, use_container_width=True)

    # Download filtered rows
    buf = io.StringIO()
    f.to_csv(buf, index=False)
    st.download_button("Download filtered rows (CSV)", data=buf.getvalue(), file_name="filtered_rows.csv", mime="text/csv")
