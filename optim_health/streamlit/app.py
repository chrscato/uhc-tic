import io
import numpy as np
import pandas as pd
import streamlit as st
import altair as alt

# ----------------------------
# App config
# ----------------------------
st.set_page_config(
    page_title="Optim Health Commercial Reimbursement",
    layout="wide"
)
st.title("Optim Health Commercial Reimbursement")

# ----------------------------
# Helpers
# ----------------------------
@st.cache_data(show_spinner=False)
def load_data():
    """
    Load production data from prod_df.parquet file.
    """
    try:
        return pd.read_parquet("prod_df.parquet")
    except FileNotFoundError:
        st.error("Production data file 'prod_df.parquet' not found. Please ensure the file exists in the same directory as this app.")
        st.stop()
    except Exception as e:
        st.error(f"Error loading production data: {str(e)}")
        st.stop()


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Light cleanup:
      - drop unnamed index cols
      - coerce rate columns to numeric
      - standardize code columns to string for filtering
      - trim whitespace in key text columns
    """
    df = df.copy()

    # Drop common junk columns
    drop_cols = [c for c in df.columns if c.lower().startswith("unnamed")]
    if drop_cols:
        df = df.drop(columns=drop_cols, errors="ignore")

    # Normalize column presence for core 3 fields
    for col in ["uhc_rate", "medicare_allowed", "GA_WC_RATE"]:
        if col not in df.columns:
            df[col] = np.nan

    # Coerce to numeric
    for col in ["uhc_rate", "medicare_allowed", "GA_WC_RATE"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Prefer 'billing_code', fall back to 'code'
    if "billing_code" in df.columns:
        code_col = "billing_code"
    elif "code" in df.columns:
        code_col = "code"
    else:
        # Create empty column to avoid crashes
        code_col = "billing_code"
        df[code_col] = pd.NA

    # Stringify codes for stable UI filtering
    df[code_col] = df[code_col].astype(str).str.strip()

    # Optional description column
    if "description" in df.columns:
        df["description"] = df["description"].astype(str).str.strip()
    else:
        df["description"] = ""

    # Add precomputed pct columns if missing (we recompute anyway)
    if "pct_of_medicare" not in df.columns:
        df["pct_of_medicare"] = np.where(
            df["medicare_allowed"].gt(0),
            df["uhc_rate"] / df["medicare_allowed"],
            np.nan
        )
    if "pct_of_ga_wc" not in df.columns:
        df["pct_of_ga_wc"] = np.where(
            df["GA_WC_RATE"].gt(0),
            df["uhc_rate"] / df["GA_WC_RATE"],
            np.nan
        )

    return df





def aggregate_for_chart(df: pd.DataFrame, code_col: str, agg: str = "median"):
    """
    Aggregate rates by billing_code using mean/median.
    """
    funcs = {"mean": "mean", "median": "median"}
    func = funcs.get(agg, "median")
    g = (
        df.groupby(code_col, dropna=False)[["uhc_rate", "medicare_allowed", "GA_WC_RATE"]]
        .agg(func)
        .reset_index()
    )
    return g


def build_grouped_bar_chart(wide_df: pd.DataFrame, code_col: str):
    """
    Build a grouped bar chart with colors:
      - UHC (blue), Medicare (grey), GA WC (green)
    """
    # Melt to long for Altair
    long_df = wide_df.melt(
        id_vars=[code_col],
        value_vars=["uhc_rate", "medicare_allowed", "GA_WC_RATE"],
        var_name="metric",
        value_name="rate"
    )

    # Label mapping and color mapping
    label_map = {
        "uhc_rate": "UHC Rate",
        "medicare_allowed": "Medicare Allowed",
        "GA_WC_RATE": "GA WC Rate"
    }
    color_domain = ["UHC Rate", "Medicare Allowed", "GA WC Rate"]
    color_range = ["#1f77b4", "#808080", "#2ca02c"]  # blue, grey, green

    long_df["metric_label"] = long_df["metric"].map(label_map)

    chart = (
        alt.Chart(long_df)
        .mark_bar()
        .encode(
            x=alt.X(f"{code_col}:N", title="Billing Code", sort=None),
            y=alt.Y("rate:Q", title="Rate"),
            color=alt.Color("metric_label:N",
                            title="Metric",
                            scale=alt.Scale(domain=color_domain, range=color_range)),
            xOffset=alt.XOffset("metric_label:N")
        )
        .properties(height=500)
        .interactive()
    )
    return chart


def format_pct(x):
    return f"{x:.0%}" if pd.notnull(x) else ""


# ----------------------------
# Sidebar — Data + Filters
# ----------------------------
with st.sidebar:
    st.header("Data")
    st.info("Loading production data from prod_df.parquet")
    
    # Load production data
    df = load_data()
    df = clean_data(df)

    # Decide code column
    code_col = "billing_code" if "billing_code" in df.columns else ("code" if "code" in df.columns else "billing_code")

    st.header("Filters (staged)")

    # Stage 1: UHC Rate as % of GA WC Range Filter
    st.subheader("UHC Rate as % of GA WC Range Filter")
    
    # Calculate pct_of_ga_wc if not already present
    if "pct_of_ga_wc" not in df.columns:
        df["pct_of_ga_wc"] = np.where(
            df["GA_WC_RATE"].gt(0),
            df["uhc_rate"] / df["GA_WC_RATE"],
            np.nan
        )
    
    # Get min/max percentages for reference
    pct_min = df["pct_of_ga_wc"].min()
    pct_max = df["pct_of_ga_wc"].max()
    
    if pd.notnull(pct_min) and pd.notnull(pct_max):
        st.write(f"UHC as % of GA WC range in data: {pct_min:.1%} - {pct_max:.1%}")
        
        col1, col2 = st.columns(2)
        with col1:
            min_pct = st.number_input(
                "Minimum % of GA WC",
                min_value=0.0,
                max_value=float(pct_max * 100),
                value=float(pct_min * 100),
                step=0.1,
                format="%.1f"
            ) / 100  # Convert to decimal
        with col2:
            max_pct = st.number_input(
                "Maximum % of GA WC",
                min_value=float(pct_min * 100),
                max_value=float(pct_max * 100) * 2,  # Allow higher values
                value=float(pct_max * 100),
                step=0.1,
                format="%.1f"
            ) / 100  # Convert to decimal
        
        # Apply the filter
        df = df[
            (df["pct_of_ga_wc"] >= min_pct) & 
            (df["pct_of_ga_wc"] <= max_pct)
        ]
    else:
        st.info("No valid percentage data found for filtering.")

    # Stage 2: Description Filter
    st.subheader("Description Filter")
    
    # Check if description column exists and has meaningful data
    if "description" in df.columns and df["description"].notna().any():
        description_filter = st.text_input(
            "Filter by description (fuzzy match)",
            placeholder="Type keywords to search in descriptions...",
            help="Enter keywords to filter rows where description contains those words (case-insensitive)"
        )
        
        if description_filter.strip():
            # Apply fuzzy match filter
            mask = df["description"].astype(str).str.contains(
                description_filter, 
                case=False, 
                na=False
            )
            df = df[mask]
            st.info(f"Filtered to {len(df)} rows containing '{description_filter}' in description")
    else:
        st.info("No description column found or no meaningful descriptions available.")

    # Stage 3: Codes (limit 5)
    # Build suggestions from filtered df
    codes_available = (
        df[code_col]
        .astype(str)
        .dropna()
        .drop_duplicates()
        .tolist()
    )

    st.caption("Pick up to 10 codes. You can also type codes separated by commas below.")
    selected_codes = st.multiselect(
        "Select codes",
        options=codes_available,
        default=[]  # Start with no selection
    )

    manual_codes = st.text_input("Or type codes (comma-separated)", value="")
    if manual_codes.strip():
        typed = [c.strip() for c in manual_codes.split(",") if c.strip()]
        combined = list(dict.fromkeys(selected_codes + typed))  # de-dupe, keep order
        if len(combined) > 10:
            st.warning("You selected more than 10 codes; only the first 10 will be used.")
            combined = combined[:10]
        selected_codes = combined
    else:
        if len(selected_codes) > 10:
            st.warning("You selected more than 10 codes; only the first 10 will be used.")
            selected_codes = selected_codes[:10]
        
        # If no codes selected, pick 10 random ones
        if not selected_codes and codes_available:
            import random
            selected_codes = random.sample(codes_available, min(10, len(codes_available)))
            st.info(f"Showing 10 random codes: {', '.join(selected_codes)}")
            
            # Add button to generate new random codes
            if st.button("🎲 Generate New Random Codes"):
                import random
                selected_codes = random.sample(codes_available, min(10, len(codes_available)))
                st.rerun()

# ----------------------------
# Main — Chart + Table
# ----------------------------
# Filter to selected codes (if any)
working = df.copy()
if selected_codes:
    working = working[working[code_col].isin(selected_codes)]

# Aggregate for chart (median keeps it robust if duplicates)
agg_df = aggregate_for_chart(working, code_col=code_col, agg="median")

# Current filter display at top
st.subheader("Current Filter")

# Create a horizontal layout for filter info
col1, col2, col3, col4 = st.columns(4)

with col1:
    # Get the percentage range for display
    pct_range_display = None
    if 'min_pct' in locals() and 'max_pct' in locals():
        pct_range_display = f"{min_pct:.1%} - {max_pct:.1%}"
    else:
        pct_range_display = "(not applied)"
    
    st.metric("UHC as % of GA WC", pct_range_display)

with col2:
    # Get the description filter for display
    description_display = None
    if 'description_filter' in locals() and description_filter.strip():
        description_display = f"'{description_filter}'"
    else:
        description_display = "(not applied)"
    
    st.metric("Description Filter", description_display)

with col3:
    # Show number of codes selected
    codes_count = len(selected_codes) if selected_codes else 0
    st.metric("Codes Selected", codes_count)

with col4:
    # Show filtered data count
    filtered_count = len(working)
    st.metric("Filtered Records", filtered_count)

# Chart and descriptions side by side
left, right = st.columns([3, 2])
with left:
    st.subheader("Rates by Code")
    if agg_df.empty or agg_df[code_col].nunique() == 0:
        st.info("No rows match your filters yet. Adjust filters or add codes.")
    else:
        chart = build_grouped_bar_chart(agg_df, code_col=code_col)
        st.altair_chart(chart, use_container_width=True)

with right:
    # Code descriptions table
    if selected_codes and "description" in working.columns:
        st.subheader("Code Descriptions")
        desc_df = working[[code_col, "description"]].drop_duplicates().head(10)
        if not desc_df.empty:
            st.dataframe(
                desc_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    code_col: st.column_config.TextColumn("Billing Code", width="medium"),
                    "description": st.column_config.TextColumn("Description", width="large")
                }
            )

# Summary Table (horizontal layout)
st.subheader("Summary Table")
if agg_df.empty or agg_df[code_col].nunique() == 0:
    st.info("No data to display in summary table.")
else:
    # Calculate percentages
    summary_df = agg_df.copy()
    summary_df["UHC as % of Medicare"] = np.where(
        summary_df["medicare_allowed"].gt(0),
        summary_df["uhc_rate"] / summary_df["medicare_allowed"],
        np.nan
    )
    summary_df["UHC as % of GA WC"] = np.where(
        summary_df["GA_WC_RATE"].gt(0),
        summary_df["uhc_rate"] / summary_df["GA_WC_RATE"],
        np.nan
    )
    
    # Add pivot count (number of rows per code)
    pivot_counts = working[code_col].value_counts().reset_index()
    pivot_counts.columns = [code_col, "Count"]
    summary_df = summary_df.merge(pivot_counts, on=code_col, how="left")
    
    # Format for display
    display_summary = summary_df.copy()
    for col in ["uhc_rate", "medicare_allowed", "GA_WC_RATE"]:
        display_summary[col] = display_summary[col].map(lambda x: f"${x:,.2f}" if pd.notnull(x) else "")
    display_summary["UHC as % of Medicare"] = display_summary["UHC as % of Medicare"].map(format_pct)
    display_summary["UHC as % of GA WC"] = display_summary["UHC as % of GA WC"].map(format_pct)
    
    st.dataframe(
        display_summary.reset_index(drop=True),
        use_container_width=True,
        hide_index=True
    )

st.subheader("Detail Table")
if working.empty:
    st.info("No data to display. Adjust filters above.")
else:
    # Define priority columns to show first
    priority_cols = [
        code_col, 
        "description", 
        "uhc_rate", 
        "medicare_allowed", 
        "GA_WC_RATE",
        "tin_value",
        "npi",
        "negotiated_type",
        "negotiation_arrangement",
        "primary_specialty",
        "CSA Title"
    ]
    
    # Create the output dataframe with only priority columns
    out = working[priority_cols].copy()
    
    # Add calculated percentage columns
    out["% of Medicare"] = np.where(
        out["medicare_allowed"].gt(0),
        out["uhc_rate"] / out["medicare_allowed"],
        np.nan
    )
    out["% of GA WC"] = np.where(
        out["GA_WC_RATE"].gt(0),
        out["uhc_rate"] / out["GA_WC_RATE"],
        np.nan
    )

    # Nice formatting for display (keep raw in chart above)
    show = out.copy()
    
    # Format currency columns
    currency_cols = ["uhc_rate", "medicare_allowed", "GA_WC_RATE"]
    for col in currency_cols:
        if col in show.columns:
            show[col] = show[col].map(lambda x: f"${x:,.2f}" if pd.notnull(x) else "")
    
    # Format percentage columns
    show["% of Medicare"] = show["% of Medicare"].map(format_pct)
    show["% of GA WC"] = show["% of GA WC"].map(format_pct)
    
    # Show column info
    final_cols = priority_cols + ["% of Medicare", "% of GA WC"]
    st.caption(f"Showing {len(final_cols)} columns: {', '.join(final_cols)}")

    st.dataframe(
        show.reset_index(drop=True),
        use_container_width=True,
        hide_index=True
    )

st.markdown(
    """
    <style>
      /* Reduce padding around charts and dataframes */
      .stAltairChart, .stDataFrame {
        padding: 0 !important;
        margin: 0 !important;
      }
      
      /* Remove gaps between columns */
      .stHorizontalBlock {
        gap: 0 !important;
      }
      
      .stHorizontalBlock > div {
        gap: 0 !important;
        margin: 0 !important;
        padding: 0 !important;
      }
    </style>
    """,
    unsafe_allow_html=True
)
