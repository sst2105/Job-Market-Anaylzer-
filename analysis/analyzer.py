import pandas as pd
from collections import Counter
import re
from database.db import get_all_jobs

SKILL_KEYWORDS = [
    "python", "sql", "excel", "power bi", "tableau", "pandas",
    "numpy", "machine learning", "deep learning", "tensorflow",
    "scikit-learn", "matplotlib", "seaborn", "spark", "hadoop",
    "aws", "gcp", "azure", "docker", "fastapi", "flask",
    "langchain", "llm", "openai", "nlp", "statistics", "r programming",
    "mongodb", "postgresql", "mysql", "data visualization",
    "business intelligence", "etl", "airflow", "git", "databricks"
]


def get_skill_frequency(df: pd.DataFrame) -> pd.DataFrame:
    """
    Count how many job postings mention each skill.
    Returns sorted DataFrame of skill counts.
    """
    skill_counts = Counter()
    
    for _, row in df.iterrows():
        skills_text = str(row.get("skills", "")).lower()
        description = str(row.get("description", "")).lower()
        combined = skills_text + " " + description
        
        for skill in SKILL_KEYWORDS:
            if skill in combined:
                skill_counts[skill] += 1
    
    skill_df = pd.DataFrame(
        skill_counts.most_common(),
        columns=["skill", "count"]
    )
    skill_df["percentage"] = (skill_df["count"] / len(df) * 100).round(1)
    return skill_df


def get_location_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    Count jobs by location/city.
    """
    # Extract city from location string
    cities = []
    for loc in df["location"].dropna():
        # Take first city if multiple listed
        city = str(loc).split(",")[0].strip()
        city = str(loc).split("/")[0].strip()
        if city and city != "nan":
            cities.append(city)
    
    location_counts = Counter(cities)
    return pd.DataFrame(
        location_counts.most_common(15),
        columns=["location", "count"]
    )


def get_company_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    Top hiring companies.
    """
    company_counts = df["company"].value_counts().head(20)
    return pd.DataFrame({
        "company": company_counts.index,
        "count": company_counts.values
    })


def get_skill_cooccurrence(df: pd.DataFrame) -> pd.DataFrame:
    """
    Find which skills appear together most often.
    Returns pivot table of skill pairs.
    
    This is the advanced analysis that shows real analytical thinking.
    """
    top_skills = ["python", "sql", "excel", "power bi", 
                  "tableau", "machine learning", "aws"]
    
    # Create binary columns for each skill
    for skill in top_skills:
        df[skill] = df.apply(
            lambda row: 1 if skill in str(row.get("skills", "")).lower() 
                        or skill in str(row.get("description", "")).lower()
            else 0, axis=1
        )
    
    # Correlation matrix shows co-occurrence
    cooccurrence = df[top_skills].corr().round(2)
    return cooccurrence


def get_experience_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fresher vs experienced breakdown.
    """
    def categorize(exp_str):
        exp_str = str(exp_str).lower()
        if any(word in exp_str for word in ["fresher", "0", "intern", "entry"]):
            return "Fresher/Intern"
        elif "1" in exp_str or "2" in exp_str:
            return "1-2 years"
        elif "3" in exp_str or "4" in exp_str or "5" in exp_str:
            return "3-5 years"
        else:
            return "5+ years"
    
    df["exp_category"] = df["experience"].apply(categorize)
    counts = df["exp_category"].value_counts()
    return pd.DataFrame({
        "category": counts.index,
        "count": counts.values
    })


def generate_insights(df: pd.DataFrame) -> list[str]:
    """
    Auto-generate text insights from the data.
    This is the storytelling layer — what does the data actually mean?
    """
    insights = []
    total = len(df)
    
    if total == 0:
        return ["No data yet — run the scraper first"]
    
    # Skill insights
    skill_df = get_skill_frequency(df)
    if not skill_df.empty:
        top_skill = skill_df.iloc[0]
        insights.append(
            f"**{top_skill['skill'].title()}** is the most demanded skill, "
            f"appearing in {top_skill['percentage']}% of all job postings"
        )
        
        # Python vs Excel comparison
        python_row = skill_df[skill_df["skill"] == "python"]
        excel_row = skill_df[skill_df["skill"] == "excel"]
        if not python_row.empty and not excel_row.empty:
            py_pct = python_row.iloc[0]["percentage"]
            ex_pct = excel_row.iloc[0]["percentage"]
            if py_pct > ex_pct:
                insights.append(
                    f"Python ({py_pct}%) now outpaces Excel ({ex_pct}%) in DA job requirements, "
                    f"signaling a shift toward programmatic analysis"
                )
    
    # Location insights
    loc_df = get_location_distribution(df)
    if not loc_df.empty:
        top_city = loc_df.iloc[0]
        insights.append(
            f"**{top_city['location']}** has the highest concentration of openings "
            f"with {top_city['count']} postings ({round(top_city['count']/total*100)}% of total)"
        )
    
    # Remote work
    remote_count = df["location"].str.contains(
        "remote|work from home|wfh", case=False, na=False
    ).sum()
    insights.append(
        f"{round(remote_count/total*100)}% of postings offer remote or hybrid work"
    )
    
    return insights