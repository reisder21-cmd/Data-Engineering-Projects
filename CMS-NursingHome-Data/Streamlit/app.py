"""
Nursing Home Staffing Dashboard

Streamlit app reading the four nh_gold tables from local Parquet
(populated by refresh_data.py).

Three tabs:
  1. National Overview  — KPIs, state ranking, HPRD/occupancy heatmaps
  2. Facility Lookup    — search a CCN/name, see facility metrics + trend
  3. Staffing Deep Dive — distributions, scatter, top/bottom 20 tables

Run:   streamlit run app.py
"""

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


CMS_MIN_HPRD = 3.48
DATA_DIR = Path(__file__).parent / "data"


# ---------------------------------------------------------------------------
# Page setup
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Nursing Home Staffing Dashboard",
    page_icon=":hospital:",
    layout="wide",
)


# ---------------------------------------------------------------------------
# Data loading (cached so re-renders are instant)
# ---------------------------------------------------------------------------
@st.cache_data
def load_data():
    facility = pd.read_parquet(DATA_DIR / "facility_summary.parquet")
    state = pd.read_parquet(DATA_DIR / "state_summary.parquet")
    facility_monthly = pd.read_parquet(DATA_DIR / "facility_monthly_trend.parquet")
    state_monthly = pd.read_parquet(DATA_DIR / "state_monthly_trend.parquet")
    return facility, state, facility_monthly, state_monthly


try:
    facility_df, state_df, facility_monthly_df, state_monthly_df = load_data()
except FileNotFoundError as e:
    st.error(
        f"Missing local data files. Run `python refresh_data.py` first.\n\n"
        f"Details: {e}"
    )
    st.stop()


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("Nursing Home Staffing Dashboard")
st.caption(
    f"Source: CMS PBJ + Provider Info  |  "
    f"{len(facility_df):,} facilities  |  "
    f"{int(facility_df['days_reported'].sum()):,} facility-days reported"
)


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_overview, tab_lookup, tab_deep = st.tabs(
    ["National Overview", "Facility Lookup", "Staffing Deep Dive"]
)


# ===========================================================================
# Tab 1: National Overview
# ===========================================================================
with tab_overview:
    # --- KPI cards ---
    weighted_hprd = (
        (facility_df["avg_hprd"] * facility_df["avg_mdscensus"]).sum()
        / facility_df["avg_mdscensus"].sum()
    )
    pct_below_min = (facility_df["avg_hprd"] < CMS_MIN_HPRD).mean()
    national_occupancy = facility_df["avg_occupancy_rate"].mean()
    avg_ratio = facility_df["avg_nurse_to_patient_ratio"].mean()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Facilities", f"{len(facility_df):,}")
    c2.metric(
        "National Avg HPRD",
        f"{weighted_hprd:.2f}",
        help="Weighted by census. Hours of nursing per resident per day.",
    )
    c3.metric(
        "% Below CMS Minimum",
        f"{pct_below_min:.1%}",
        help=f"Facilities with HPRD below the {CMS_MIN_HPRD} CMS threshold.",
    )
    c4.metric(
        "Avg Occupancy",
        f"{national_occupancy:.1%}",
        help="Census / certified beds, averaged across facilities.",
    )

    st.markdown("---")

    # --- State ranking bar chart ---
    st.subheader("State Ranking by Average HPRD")
    st.caption("Higher HPRD = more nursing hours per resident. CMS minimum is 3.48.")

    ranked = state_df.sort_values("weighted_avg_hprd", ascending=True).copy()
    ranked["below_minimum"] = ranked["weighted_avg_hprd"] < CMS_MIN_HPRD

    fig = px.bar(
        ranked,
        x="weighted_avg_hprd",
        y="state",
        orientation="h",
        color="weighted_avg_hprd",
        color_continuous_scale="RdYlGn",
        labels={"weighted_avg_hprd": "Weighted Avg HPRD", "state": "State"},
        hover_data={"facility_count": True, "weighted_avg_hprd": ":.2f"},
        height=900,
    )
    fig.add_vline(x=CMS_MIN_HPRD, line_dash="dash", line_color="red",
                  annotation_text="CMS Min (3.48)", annotation_position="top")
    fig.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # --- Ownership type breakdown ---
    st.subheader("By Ownership Type")
    st.caption("How the average facility differs by who owns it. Weighted by census.")

    def _weighted_mean(df, value_col, weight_col):
        d = df.dropna(subset=[value_col, weight_col])
        if d[weight_col].sum() == 0:
            return None
        return (d[value_col] * d[weight_col]).sum() / d[weight_col].sum()

    ownership_rows = []
    for own_type, group in facility_df.dropna(subset=["ownership_type"]).groupby("ownership_type"):
        ownership_rows.append({
            "ownership_type": own_type,
            "facility_count": len(group),
            "weighted_hprd": _weighted_mean(group, "avg_hprd", "avg_mdscensus"),
            "weighted_occupancy": _weighted_mean(group, "avg_occupancy_rate", "avg_mdscensus"),
        })
    ownership_df = (
        pd.DataFrame(ownership_rows)
        .dropna(subset=["weighted_hprd"])
        .sort_values("weighted_hprd", ascending=False)
    )

    o1, o2 = st.columns(2)

    with o1:
        st.markdown("**Weighted Avg HPRD by Ownership**")
        fig = px.bar(
            ownership_df, x="ownership_type", y="weighted_hprd",
            color="weighted_hprd", color_continuous_scale="RdYlGn",
            labels={"weighted_hprd": "Weighted Avg HPRD",
                    "ownership_type": "Ownership Type"},
            hover_data={"facility_count": True, "weighted_hprd": ":.2f"},
        )
        fig.add_hline(y=CMS_MIN_HPRD, line_dash="dash", line_color="red",
                      annotation_text="CMS Min")
        fig.update_layout(coloraxis_showscale=False, height=400, xaxis_tickangle=-30)
        st.plotly_chart(fig, use_container_width=True)

    with o2:
        st.markdown("**Weighted Avg Occupancy by Ownership**")
        fig = px.bar(
            ownership_df, x="ownership_type", y="weighted_occupancy",
            color="weighted_occupancy", color_continuous_scale="Blues",
            labels={"weighted_occupancy": "Weighted Avg Occupancy",
                    "ownership_type": "Ownership Type"},
            hover_data={"facility_count": True, "weighted_occupancy": ":.2%"},
        )
        fig.update_yaxes(tickformat=".0%")
        fig.update_layout(coloraxis_showscale=False, height=400, xaxis_tickangle=-30)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # --- Heatmaps ---
    st.subheader("State x Month Heatmaps")

    heat_col1, heat_col2 = st.columns(2)

    with heat_col1:
        st.markdown("**Weighted Avg HPRD**")
        hprd_pivot = (
            state_monthly_df.pivot(index="state", columns="year_month",
                                   values="weighted_avg_hprd")
            .sort_index()
        )
        fig = px.imshow(
            hprd_pivot,
            color_continuous_scale="RdYlGn",
            aspect="auto",
            labels={"color": "HPRD"},
        )
        fig.update_layout(height=900, xaxis_title="Month", yaxis_title="State")
        st.plotly_chart(fig, use_container_width=True)

    with heat_col2:
        st.markdown("**Weighted Avg Occupancy**")
        occ_pivot = (
            state_monthly_df.pivot(index="state", columns="year_month",
                                   values="weighted_avg_occupancy_rate")
            .sort_index()
        )
        fig = px.imshow(
            occ_pivot,
            color_continuous_scale="Blues",
            aspect="auto",
            labels={"color": "Occupancy"},
        )
        fig.update_layout(height=900, xaxis_title="Month", yaxis_title="State")
        st.plotly_chart(fig, use_container_width=True)


# ===========================================================================
# Tab 2: Facility Lookup
# ===========================================================================
with tab_lookup:
    st.subheader("Find a Facility")

    search = st.text_input(
        "Search by CCN or facility name",
        placeholder="e.g., 145446 or 'Manor'",
    )

    if not search:
        st.info("Enter a CCN or part of a facility name to look up details.")
    else:
        mask = (
            facility_df["ccn"].str.contains(search, case=False, na=False)
            | facility_df["provider_name"].str.contains(search, case=False, na=False)
        )
        matches = facility_df[mask]

        if matches.empty:
            st.warning("No matches found.")
        else:
            st.caption(f"{len(matches)} match{'es' if len(matches) > 1 else ''}")

            options = [
                f"{row['ccn']}  -  {row['provider_name']}  ({row['state']})"
                for _, row in matches.head(50).iterrows()
            ]
            selected = st.selectbox("Select a facility", options)
            ccn = selected.split("  -  ")[0].strip()

            facility = facility_df[facility_df["ccn"] == ccn].iloc[0]

            # --- Facility info card ---
            st.markdown(f"### {facility['provider_name']}")
            st.caption(
                f"CCN {facility['ccn']}  |  "
                f"{facility['city']}, {facility['state']}  |  "
                f"{facility['ownership_type']}  |  "
                f"{int(facility['certified_beds']) if pd.notna(facility['certified_beds']) else '?'} beds"
            )

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Avg HPRD", f"{facility['avg_hprd']:.2f}",
                      delta=f"{facility['avg_hprd'] - weighted_hprd:+.2f} vs national")
            m2.metric("Occupancy",
                      f"{facility['avg_occupancy_rate']:.1%}"
                      if pd.notna(facility["avg_occupancy_rate"]) else "n/a")
            m3.metric("Nurse:Patient",
                      f"1 : {facility['avg_nurse_to_patient_ratio']:.1f}"
                      if pd.notna(facility["avg_nurse_to_patient_ratio"]) else "n/a",
                      help="Residents per concurrent nurse on shift (avg over period)")
            m4.metric("Contractor %",
                      f"{facility['avg_contractor_pct']:.1%}"
                      if pd.notna(facility["avg_contractor_pct"]) else "n/a")

            r1, r2, r3, r4 = st.columns(4)
            r1.metric("Overall Rating",
                      f"{int(facility['overall_rating'])} / 5"
                      if pd.notna(facility["overall_rating"]) else "n/a")
            r2.metric("Staffing Rating",
                      f"{int(facility['staffing_rating'])} / 5"
                      if pd.notna(facility["staffing_rating"]) else "n/a")
            r3.metric("Days Reported", f"{int(facility['days_reported']):,}")
            r4.metric("Days Below CMS Min",
                      f"{int(facility['days_below_cms_minimum']):,}",
                      delta=f"{facility['pct_days_below_cms_minimum']:.0%} of period",
                      delta_color="inverse")

            st.markdown("---")

            # --- Trend lines ---
            trend = (
                facility_monthly_df[facility_monthly_df["provnum"] == ccn]
                .sort_values("year_month")
            )
            if trend.empty:
                st.info("No monthly trend data available for this facility.")
            else:
                trend_col1, trend_col2 = st.columns(2)

                with trend_col1:
                    st.markdown("**HPRD Trend**")
                    fig = px.line(
                        trend, x="year_month", y="avg_hprd", markers=True,
                        labels={"year_month": "Month", "avg_hprd": "Avg HPRD"},
                    )
                    fig.add_hline(y=CMS_MIN_HPRD, line_dash="dash", line_color="red",
                                  annotation_text="CMS Min")
                    # Add state average for context
                    state_trend = (
                        state_monthly_df[state_monthly_df["state"] == facility["state"]]
                        .sort_values("year_month")
                    )
                    fig.add_scatter(
                        x=state_trend["year_month"],
                        y=state_trend["weighted_avg_hprd"],
                        mode="lines", line_dash="dot", name=f"{facility['state']} avg",
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with trend_col2:
                    st.markdown("**Occupancy Trend**")
                    fig = px.line(
                        trend, x="year_month", y="avg_occupancy_rate", markers=True,
                        labels={"year_month": "Month", "avg_occupancy_rate": "Occupancy"},
                    )
                    fig.update_yaxes(tickformat=".0%")
                    st.plotly_chart(fig, use_container_width=True)


# ===========================================================================
# Tab 3: Staffing Deep Dive
# ===========================================================================
with tab_deep:
    st.subheader("Distributions Across Facilities")

    d1, d2 = st.columns(2)

    with d1:
        st.markdown("**HPRD Distribution**")
        fig = px.histogram(
            facility_df, x="avg_hprd", nbins=60,
            labels={"avg_hprd": "Avg HPRD"},
        )
        fig.add_vline(x=CMS_MIN_HPRD, line_dash="dash", line_color="red",
                      annotation_text=f"CMS Min ({CMS_MIN_HPRD})")
        st.plotly_chart(fig, use_container_width=True)

    with d2:
        st.markdown("**Occupancy Rate Distribution**")
        fig = px.histogram(
            facility_df.dropna(subset=["avg_occupancy_rate"]),
            x="avg_occupancy_rate", nbins=60,
            labels={"avg_occupancy_rate": "Avg Occupancy"},
        )
        fig.update_xaxes(tickformat=".0%")
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # --- Scatter ---
    st.subheader("Staffing vs Occupancy")
    st.caption(
        "Each dot is one facility. Size = certified beds. "
        "Color = ownership type."
    )

    scatter_df = facility_df.dropna(
        subset=["avg_hprd", "avg_occupancy_rate", "certified_beds", "ownership_type"]
    )
    fig = px.scatter(
        scatter_df,
        x="avg_occupancy_rate",
        y="avg_hprd",
        size="certified_beds",
        color="ownership_type",
        hover_data=["provider_name", "state", "ccn"],
        labels={
            "avg_occupancy_rate": "Avg Occupancy",
            "avg_hprd": "Avg HPRD",
        },
        opacity=0.5,
        height=600,
    )
    fig.add_hline(y=CMS_MIN_HPRD, line_dash="dash", line_color="red",
                  annotation_text="CMS Min")
    fig.update_xaxes(tickformat=".0%")
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # --- Top / bottom tables ---
    st.subheader("Facility Rankings")
    rank_col1, rank_col2 = st.columns(2)

    display_cols = [
        "ccn", "provider_name", "state", "certified_beds",
        "avg_hprd", "avg_occupancy_rate", "overall_rating",
    ]
    rename = {
        "ccn": "CCN",
        "provider_name": "Facility",
        "state": "State",
        "certified_beds": "Beds",
        "avg_hprd": "HPRD",
        "avg_occupancy_rate": "Occupancy",
        "overall_rating": "Rating",
    }

    with rank_col1:
        st.markdown("**Top 20 by HPRD**")
        top = (
            facility_df.dropna(subset=["avg_hprd"])
            .nlargest(20, "avg_hprd")[display_cols]
            .rename(columns=rename)
        )
        st.dataframe(
            top.style.format({
                "HPRD": "{:.2f}",
                "Occupancy": "{:.1%}",
                "Rating": "{:.0f}",
            }),
            use_container_width=True,
            hide_index=True,
        )

    with rank_col2:
        st.markdown("**Bottom 20 by HPRD** (only facilities with census >= 5)")
        bottom = (
            facility_df.dropna(subset=["avg_hprd"])
            .query("avg_mdscensus >= 5")
            .nsmallest(20, "avg_hprd")[display_cols]
            .rename(columns=rename)
        )
        st.dataframe(
            bottom.style.format({
                "HPRD": "{:.2f}",
                "Occupancy": "{:.1%}",
                "Rating": "{:.0f}",
            }),
            use_container_width=True,
            hide_index=True,
        )


# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown("---")
st.caption(
    "Data flows: CMS PBJ + Provider Info -> S3 bronze -> silver star schema "
    "-> gold pre-aggregates -> this dashboard. "
    "DQ checks gate the silver -> gold transition."
)
