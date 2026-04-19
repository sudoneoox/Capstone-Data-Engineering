"""
Job Market Intelligence Platform — Dashboard
Reads from gold layer CSVs exported from DuckDB.
Run: streamlit run dashboard/app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# INFO: CONFIG
st.set_page_config(
    page_title="Job Market Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

DATA_DIR = Path("data/dashboard")


@st.cache_data
def load_csv(name: str) -> pd.DataFrame:
    path = DATA_DIR / f"{name}.csv"
    if not path.exists():
        st.error(f"Missing: {path}. Run the export script first.")
        return pd.DataFrame()
    df = pd.read_csv(path)
    # Auto-parse date columns
    for col in df.columns:
        if "month" in col or "date" in col or "posting" in col and "earliest" in col:
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            except Exception:
                pass
    return df


# INFO: SIDEBAR — navigation

st.sidebar.title("📊 Job market intelligence")
st.sidebar.caption("Data engineering capstone project")

page = st.sidebar.radio(
    "Dashboard",
    ["Market pulse", "Role deep-dive", "Metro compare", "Skills tracker"],
    label_visibility="collapsed",
)

st.sidebar.markdown("---")
st.sidebar.caption("Sources: O*NET 30.2 · BLS · FRED · Adzuna · ACS · Kaggle LinkedIn")


# HELPERS


def metric_card(label: str, value, subtitle: str = ""):
    """Render a styled metric."""
    st.metric(label=label, value=value, help=subtitle)


def fmt_salary(val) -> str:
    if pd.isna(val) or val is None:
        return "N/A"
    return f"${int(val):,}"


def fmt_pct(val) -> str:
    if pd.isna(val) or val is None:
        return "N/A"
    return f"{val * 100:.1f}%"


def fmt_number(val) -> str:
    if pd.isna(val) or val is None:
        return "N/A"
    if val >= 1_000_000:
        return f"{val / 1_000_000:.2f}M"
    if val >= 1_000:
        return f"{val / 1_000:.1f}K"
    return f"{int(val):,}"


# INFO: PAGE 1: MARKET PULSE
# ======================================================
if page == "Market pulse":
    st.title("Market pulse")
    st.caption("Cross-source job market health and macroeconomic context")

    snapshot = load_csv("gold_market_snapshot")
    trends = load_csv("gold_market_trends")
    econ = load_csv("gold_economic_dashboard")

    if snapshot.empty:
        st.stop()

    # ---- KPI row ----
    total_postings = snapshot["total_postings"].sum()
    total_companies = snapshot["unique_companies"].sum()

    # Adzuna row for salary
    adzuna_row = snapshot[snapshot["data_source"] == "adzuna"]
    linkedin_row = snapshot[snapshot["data_source"] == "kaggle_linkedin"]

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Total postings", fmt_number(total_postings))
    with c2:
        st.metric("Unique companies", fmt_number(total_companies))
    with c3:
        if not adzuna_row.empty:
            st.metric(
                "Adzuna median salary",
                fmt_salary(adzuna_row.iloc[0].get("median_salary")),
                help=f"{int(adzuna_row.iloc[0].get('postings_with_salary', 0)):,} postings with salary",
            )
        else:
            st.metric("Adzuna median salary", "N/A")
    with c4:
        if not linkedin_row.empty:
            st.metric(
                "LinkedIn median salary",
                fmt_salary(linkedin_row.iloc[0].get("median_salary")),
                help=f"{int(linkedin_row.iloc[0].get('postings_with_salary', 0)):,} postings with salary",
            )
        else:
            st.metric("LinkedIn median salary", "N/A")

    st.markdown("---")

    # ---- Source comparison ----
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Posting volume by source")
        fig = px.bar(
            snapshot.sort_values("total_postings", ascending=True),
            x="total_postings",
            y="data_source",
            orientation="h",
            color="is_snapshot",
            color_discrete_map={True: "#B5D4F4", False: "#5DCAA5"},
            labels={
                "total_postings": "Total postings",
                "data_source": "",
                "is_snapshot": "Snapshot",
            },
        )
        fig.update_layout(
            height=280,
            margin=dict(l=0, r=0, t=10, b=0),
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Coverage by source")
        coverage_data = snapshot[
            [
                "data_source",
                "postings_with_salary",
                "remote_postings",
                "soc_matched_postings",
            ]
        ].copy()
        coverage_data = coverage_data.melt(
            id_vars="data_source", var_name="metric", value_name="count"
        )
        coverage_data["metric"] = coverage_data["metric"].map(
            {
                "postings_with_salary": "Has salary",
                "remote_postings": "Remote",
                "soc_matched_postings": "SOC matched",
            }
        )
        fig2 = px.bar(
            coverage_data,
            x="data_source",
            y="count",
            color="metric",
            barmode="group",
            labels={"data_source": "", "count": "Postings", "metric": ""},
        )
        fig2.update_layout(
            height=280,
            margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig2, use_container_width=True)

    # ---- Monthly trends ----
    if not trends.empty:
        st.markdown("---")
        st.subheader("Monthly posting trends (non-snapshot sources)")
        fig3 = px.line(
            trends.sort_values("report_month"),
            x="report_month",
            y="total_postings",
            color="data_source",
            markers=True,
            labels={
                "report_month": "",
                "total_postings": "Postings",
                "data_source": "Source",
            },
        )
        fig3.update_layout(
            height=300,
            margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig3, use_container_width=True)

    # ---- Economic indicators ----
    if not econ.empty:
        st.markdown("---")
        st.subheader("Economic indicators")

        econ["report_month"] = pd.to_datetime(econ["report_month"], errors="coerce")

        categories = sorted(econ["category"].dropna().unique())
        selected_cat = st.selectbox("Category", categories, index=0)
        econ_filtered = econ[econ["category"] == selected_cat]

        series_list = sorted(econ_filtered["series_name"].dropna().unique())

        # Show up to 4 charts in a 2x2 grid
        charts_per_row = 2
        for i in range(0, min(len(series_list), 4), charts_per_row):
            cols = st.columns(charts_per_row)
            for j, col in enumerate(cols):
                idx = i + j
                if idx >= len(series_list):
                    break
                series = series_list[idx]
                series_data = econ_filtered[
                    econ_filtered["series_name"] == series
                ].sort_values("report_month")
                with col:
                    fig = px.line(
                        series_data,
                        x="report_month",
                        y="value",
                        title=series.title(),
                        labels={"report_month": "", "value": ""},
                    )
                    fig.update_layout(
                        height=250,
                        margin=dict(l=0, r=0, t=35, b=0),
                        showlegend=False,
                    )
                    st.plotly_chart(fig, use_container_width=True)


# INFO: PAGE 2: ROLE DEEP-DIVE
# ======================================================

elif page == "Role deep-dive":
    st.title("Role deep-dive")
    st.caption("Per-occupation analytics with salary, skills, and education data")

    roles = load_csv("gold_role_analysis")
    skills = load_csv("gold_role_top_skills")

    if roles.empty:
        st.stop()

    # ---- SOC prefix filter ----
    roles["soc_prefix"] = roles["onet_soc_code"].str[:2]
    prefix_labels = {
        "15": "15 — Computer & math",
        "17": "17 — Architecture & engineering",
        "13": "13 — Business & financial",
        "11": "11 — Management",
        "29": "29 — Healthcare",
        "ALL": "All occupations",
    }
    selected_prefix = st.selectbox(
        "Filter by SOC group",
        list(prefix_labels.keys()),
        format_func=lambda x: prefix_labels[x],
        index=0,
    )

    if selected_prefix != "ALL":
        roles_filtered = roles[roles["soc_prefix"] == selected_prefix].copy()
    else:
        roles_filtered = roles.copy()

    roles_filtered = roles_filtered.sort_values("total_postings", ascending=False)

    # ---- KPIs for filtered group ----
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Occupations", len(roles_filtered))
    with c2:
        st.metric("Total postings", fmt_number(roles_filtered["total_postings"].sum()))
    with c3:
        med_sal = roles_filtered["median_salary"].dropna().median()
        st.metric("Median salary (of medians)", fmt_salary(med_sal))
    with c4:
        avg_remote = roles_filtered["pct_remote"].dropna().mean()
        st.metric("Avg % remote", fmt_pct(avg_remote))

    st.markdown("---")

    # ---- Role table ----
    st.subheader("Top occupations by posting volume")
    display_cols = [
        "occupation_title",
        "total_postings",
        "median_salary",
        "salary_p25",
        "salary_p75",
        "pct_remote",
        "most_common_experience_level",
        "job_zone",
    ]
    available_cols = [c for c in display_cols if c in roles_filtered.columns]
    top_roles = roles_filtered.head(30)[available_cols].copy()

    # Format for display
    for col in ["median_salary", "salary_p25", "salary_p75"]:
        if col in top_roles.columns:
            top_roles[col] = top_roles[col].apply(
                lambda x: fmt_salary(x) if pd.notna(x) else ""
            )
    if "pct_remote" in top_roles.columns:
        top_roles["pct_remote"] = top_roles["pct_remote"].apply(
            lambda x: fmt_pct(x) if pd.notna(x) else ""
        )

    top_roles = top_roles.rename(
        columns={
            "occupation_title": "Occupation",
            "total_postings": "Postings",
            "median_salary": "Median salary",
            "salary_p25": "P25",
            "salary_p75": "P75",
            "pct_remote": "% Remote",
            "most_common_experience_level": "Experience",
            "job_zone": "Zone",
        }
    )
    st.dataframe(top_roles, use_container_width=True, hide_index=True, height=400)

    # ---- Skills for selected role ----
    if not skills.empty:
        st.markdown("---")
        st.subheader("Top skills for a specific role")

        role_options = roles_filtered["occupation_title"].tolist()
        if role_options:
            selected_role = st.selectbox("Select occupation", role_options[:50])

            role_skills = skills[
                skills["occupation_title"] == selected_role
            ].sort_values("skill_rank")

            if not role_skills.empty:
                col_left, col_right = st.columns([2, 1])

                with col_left:
                    fig = px.bar(
                        role_skills.head(15),
                        x="mention_count",
                        y="skill_name",
                        orientation="h",
                        labels={"mention_count": "Mentions", "skill_name": ""},
                        color="pct_of_postings",
                        color_continuous_scale="Teal",
                    )
                    fig.update_layout(
                        height=450,
                        margin=dict(l=0, r=0, t=10, b=0),
                        yaxis=dict(autorange="reversed"),
                        coloraxis_colorbar_title="% of postings",
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with col_right:
                    # Role details card
                    role_row = roles_filtered[
                        roles_filtered["occupation_title"] == selected_role
                    ].iloc[0]

                    st.markdown(f"**{selected_role}**")
                    st.markdown(f"Job zone: **{role_row.get('job_zone', 'N/A')}**")
                    st.markdown(
                        f"Total postings: **{fmt_number(role_row.get('total_postings', 0))}**"
                    )
                    st.markdown(
                        f"Median salary: **{fmt_salary(role_row.get('median_salary'))}**"
                    )
                    st.markdown(f"Remote: **{fmt_pct(role_row.get('pct_remote'))}**")
                    st.markdown(
                        f"Experience: **{role_row.get('most_common_experience_level', 'N/A')}**"
                    )

                    if pd.notna(role_row.get("pct_bachelors_degree")):
                        st.markdown("---")
                        st.markdown("**Education (O\\*NET)**")
                        st.markdown(
                            f"Bachelor's: {role_row['pct_bachelors_degree']:.1f}%"
                        )
                        if pd.notna(role_row.get("pct_masters_degree")):
                            st.markdown(
                                f"Master's: {role_row['pct_masters_degree']:.1f}%"
                            )
            else:
                st.info("No skill data for this occupation.")

# INFO: PAGE 3: METRO COMPARE
# ======================================================

elif page == "Metro compare":
    st.title("Metro compare")
    st.caption("Geographic job market comparison with ACS demographic context")

    metro = load_csv("gold_metro_comparison")

    if metro.empty:
        st.stop()

    # ---- KPI row ----
    matched_metros = metro[metro["cbsa_fips"].notna()]
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("States with data", len(metro))
    with c2:
        st.metric("Matched to ACS metros", len(matched_metros))
    with c3:
        st.metric(
            "Total geo-tagged postings", fmt_number(metro["total_postings"].sum())
        )

    st.markdown("---")

    # ---- Scatter plot: salary vs cost of living ----
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Posting volume by state")
        fig = px.bar(
            metro.sort_values("total_postings", ascending=True),
            x="total_postings",
            y="state",
            orientation="h",
            color="median_salary",
            color_continuous_scale="Teal",
            labels={
                "total_postings": "Postings",
                "state": "",
                "median_salary": "Median salary",
            },
        )
        fig.update_layout(
            height=450,
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Salary vs cost of living")
        scatter_data = matched_metros.dropna(
            subset=["median_salary", "median_household_income"]
        )
        if not scatter_data.empty:
            fig2 = px.scatter(
                scatter_data,
                x="median_household_income",
                y="median_salary",
                size="total_postings",
                text="state",
                hover_data=["metro_name", "salary_to_income_ratio"],
                labels={
                    "median_household_income": "ACS median household income",
                    "median_salary": "Median tech salary",
                },
                color="salary_to_income_ratio",
                color_continuous_scale="RdYlGn",
            )
            # Add break-even line
            max_val = max(
                scatter_data["median_household_income"].max(),
                scatter_data["median_salary"].max(),
            )
            fig2.add_trace(
                go.Scatter(
                    x=[0, max_val],
                    y=[0, max_val],
                    mode="lines",
                    line=dict(dash="dash", color="gray", width=1),
                    name="Ratio = 1.0",
                    showlegend=False,
                )
            )
            fig2.update_traces(
                textposition="top center", selector=dict(mode="markers+text")
            )
            fig2.update_layout(
                height=450,
                margin=dict(l=0, r=0, t=10, b=0),
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No metros with both salary and ACS data.")

    # ---- Metro detail table ----
    if not matched_metros.empty:
        st.markdown("---")
        st.subheader("ACS metro profiles")
        detail_cols = [
            "state",
            "metro_name",
            "total_postings",
            "median_salary",
            "salary_to_income_ratio",
            "median_household_income",
            "pct_bachelors_or_higher",
            "pct_work_from_home",
            "pct_information_industry",
            "acs_unemployment_rate",
        ]
        available = [c for c in detail_cols if c in matched_metros.columns]
        display_df = matched_metros[available].copy()
        display_df = display_df.rename(
            columns={
                "state": "State",
                "metro_name": "Metro",
                "total_postings": "Postings",
                "median_salary": "Median salary",
                "salary_to_income_ratio": "Salary/Income",
                "median_household_income": "HH Income",
                "pct_bachelors_or_higher": "% BA+",
                "pct_work_from_home": "% WFH",
                "pct_information_industry": "% Info",
                "acs_unemployment_rate": "Unemp %",
            }
        )
        st.dataframe(display_df, use_container_width=True, hide_index=True)

# INFO: PAGE 4: SKILLS TRACKER
# ======================================================

elif page == "Skills tracker":
    st.title("Skills tracker")
    st.caption("Skills demand analysis and O*NET gap identification")

    skills_demand = load_csv("gold_skills_demand")
    gap = load_csv("gold_occupation_skills_profile")

    if skills_demand.empty and gap.empty:
        st.stop()

    # ---- Top skills ----
    if not skills_demand.empty:
        st.subheader("Top skills by total mentions")

        top_n = st.slider("Number of skills to show", 10, 50, 20)

        skills_agg = (
            skills_demand.groupby("skill_name")
            .agg(
                total_mentions=("mention_count", "sum"),
                total_distinct=("distinct_postings", "sum"),
                months_active=("report_month", "nunique"),
            )
            .sort_values("total_mentions", ascending=False)
            .head(top_n)
            .reset_index()
        )

        fig = px.bar(
            skills_agg.sort_values("total_mentions", ascending=True),
            x="total_mentions",
            y="skill_name",
            orientation="h",
            labels={"total_mentions": "Total mentions", "skill_name": ""},
            color="total_mentions",
            color_continuous_scale="Teal",
        )
        fig.update_layout(
            height=max(400, top_n * 22),
            margin=dict(l=0, r=0, t=10, b=0),
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig, use_container_width=True)

    # ---- Gap analysis ----
    if not gap.empty:
        st.markdown("---")
        st.subheader("O*NET vs market skills gap")

        col_left, col_right = st.columns([1, 2])

        with col_left:
            gap_summary = gap["gap_direction"].value_counts().reset_index()
            gap_summary.columns = ["gap_direction", "count"]
            color_map = {
                "market_only": "#5DCAA5",
                "onet_only": "#B5D4F4",
                "aligned": "#AFA9EC",
            }
            fig = px.pie(
                gap_summary,
                values="count",
                names="gap_direction",
                color="gap_direction",
                color_discrete_map=color_map,
                hole=0.5,
            )
            fig.update_layout(
                height=300,
                margin=dict(l=0, r=0, t=10, b=0),
                legend=dict(orientation="h", yanchor="bottom", y=-0.2),
            )
            st.plotly_chart(fig, use_container_width=True)

        with col_right:
            st.markdown("**Emerging tools (market_only, tech roles)**")
            st.caption("Skills employers ask for but O*NET hasn't formally cataloged")

            # Filter to tech SOC codes and market_only
            tech_gap = gap[
                (gap["gap_direction"] == "market_only")
                & (gap["onet_soc_code"].str.startswith("15"))
            ].copy()

            if not tech_gap.empty:
                top_emerging = (
                    tech_gap.groupby("skill_name")["posting_mention_count"]
                    .sum()
                    .sort_values(ascending=False)
                    .head(15)
                    .reset_index()
                )
                fig2 = px.bar(
                    top_emerging.sort_values("posting_mention_count", ascending=True),
                    x="posting_mention_count",
                    y="skill_name",
                    orientation="h",
                    labels={"posting_mention_count": "Mentions", "skill_name": ""},
                    color_discrete_sequence=["#5DCAA5"],
                )
                fig2.update_layout(
                    height=400,
                    margin=dict(l=0, r=0, t=10, b=0),
                )
                st.plotly_chart(fig2, use_container_width=True)
            else:
                st.info(
                    "No tech-specific emerging skills found. Try broadening the SOC filter."
                )

        # ---- Gap detail table ----
        st.markdown("---")
        st.subheader("Explore gap by occupation")

        gap_direction_filter = st.selectbox(
            "Gap direction", ["market_only", "onet_only", "aligned"]
        )

        # Occupation filter
        occ_list = sorted(gap["occupation_title"].dropna().unique())
        selected_occ = st.selectbox("Occupation", occ_list, index=0)

        detail = gap[
            (gap["gap_direction"] == gap_direction_filter)
            & (gap["occupation_title"] == selected_occ)
        ].sort_values("posting_mention_count", ascending=False)

        if not detail.empty:
            show_cols = [
                "skill_name",
                "posting_mention_count",
                "posting_pct_of_postings",
                "onet_is_hot_technology",
                "onet_is_in_demand",
            ]
            available = [c for c in show_cols if c in detail.columns]
            st.dataframe(
                detail[available].head(30), use_container_width=True, hide_index=True
            )
        else:
            st.info(f"No {gap_direction_filter} skills for {selected_occ}.")
