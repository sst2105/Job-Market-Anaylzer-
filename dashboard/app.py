import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db import get_all_jobs, get_job_count
from analysis.analyzer import (
    get_skill_frequency,
    get_location_distribution,
    get_company_distribution,
    get_experience_distribution,
    get_skill_cooccurrence,
    generate_insights
)

# Page config
st.set_page_config(
    page_title="India Tech Job Market Analyzer",
    page_icon="📊",
    layout="wide"
)

# Header
st.title("📊 India Tech Job Market Analyzer")
st.markdown("*Real-time analysis of tech job postings — scraped live from Internshala, Remotive & more*")
st.divider()

# Load data
df = get_all_jobs()
total = get_job_count()

if total == 0:
    st.warning("No jobs in database yet. Run `python main.py` first to scrape jobs.")
    st.stop()

# ── TOP METRICS ──
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Postings Analyzed", total)
with col2:
    sources = df["source"].nunique()
    st.metric("Data Sources", sources)
with col3:
    companies = df["company"].nunique()
    st.metric("Unique Companies", companies)
with col4:
    remote = df["location"].str.contains("remote|work from home|wfh", case=False, na=False).sum()
    st.metric("Remote Roles", remote)

st.divider()

# ── INSIGHTS ──
st.subheader("🔍 Key Insights")
insights = generate_insights(df)
for insight in insights:
    st.markdown(f"- {insight}")

st.divider()

# ── SKILL FREQUENCY ──
st.subheader("🛠️ Most In-Demand Skills")
st.markdown("*Which skills appear most frequently across all job postings*")

skill_df = get_skill_frequency(df)

if not skill_df.empty:
    top_skills = skill_df.head(20)
    
    fig = px.bar(
        top_skills,
        x="count",
        y="skill",
        orientation="h",
        color="percentage",
        color_continuous_scale="blues",
        labels={"count": "Number of Postings", "skill": "Skill", "percentage": "% of Jobs"},
        title="Top 20 Skills by Job Posting Frequency"
    )
    fig.update_layout(
        height=500,
        yaxis={"categoryorder": "total ascending"},
        showlegend=False
    )
    st.plotly_chart(fig, use_container_width=True)

    # Skill table
    with st.expander("View full skill data"):
        st.dataframe(
            skill_df.rename(columns={
                "skill": "Skill",
                "count": "Postings",
                "percentage": "% of Jobs"
            }),
            use_container_width=True
        )

st.divider()

# ── LOCATION ──
st.subheader("📍 Where Are the Jobs?")

col1, col2 = st.columns(2)

with col1:
    loc_df = get_location_distribution(df)
    if not loc_df.empty:
        fig = px.bar(
            loc_df.head(10),
            x="location",
            y="count",
            color="count",
            color_continuous_scale="teal",
            title="Top 10 Locations",
            labels={"location": "Location", "count": "Postings"}
        )
        fig.update_layout(showlegend=False, xaxis_tickangle=-30)
        st.plotly_chart(fig, use_container_width=True)

with col2:
    # Source distribution
    source_counts = df["source"].value_counts().reset_index()
    source_counts.columns = ["source", "count"]
    fig = px.pie(
        source_counts,
        values="count",
        names="source",
        title="Jobs by Source Platform",
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── SKILL CO-OCCURRENCE ──
st.subheader("🔗 Skill Co-occurrence Matrix")
st.markdown("*Which skills are most likely to appear together in the same job posting*")

try:
    cooccurrence = get_skill_cooccurrence(df)
    if not cooccurrence.empty:
        fig = px.imshow(
            cooccurrence,
            color_continuous_scale="RdBu",
            title="Skill Correlation Heatmap",
            aspect="auto"
        )
        fig.update_layout(height=450)
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Values close to 1.0 mean skills frequently appear together. Values near 0 mean they rarely co-occur.")
except Exception as e:
    st.info("Need more data for co-occurrence analysis")

st.divider()

# ── TOP COMPANIES ──
st.subheader("🏢 Top Hiring Companies")

company_df = get_company_distribution(df)
if not company_df.empty:
    fig = px.bar(
        company_df.head(15),
        x="company",
        y="count",
        color="count",
        color_continuous_scale="purples",
        title="Companies with Most Openings",
        labels={"company": "Company", "count": "Postings"}
    )
    fig.update_layout(showlegend=False, xaxis_tickangle=-30)
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── RAW DATA ──
st.subheader("📋 Raw Job Data")
with st.expander("View all scraped jobs"):
    display_cols = ["title", "company", "location", "skills", "salary", "source"]
    available_cols = [c for c in display_cols if c in df.columns]
    st.dataframe(df[available_cols], use_container_width=True)

# Footer
st.divider()
st.caption(f"Data scraped from {sources} sources | {total} postings analyzed | Built with Python, BeautifulSoup, Pandas, Plotly & Streamlit")