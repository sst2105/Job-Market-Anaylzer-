import requests
import time
import random
from database.db import insert_job

SKILL_KEYWORDS = [
    "python", "sql", "excel", "power bi", "tableau", "pandas",
    "numpy", "machine learning", "deep learning", "tensorflow",
    "scikit-learn", "matplotlib", "seaborn", "spark", "hadoop",
    "aws", "gcp", "azure", "docker", "fastapi", "flask",
    "langchain", "llm", "openai", "nlp", "statistics",
    "mongodb", "postgresql", "mysql", "data visualization",
    "business intelligence", "etl", "airflow", "git", "databricks",
    "r programming", "looker", "dbt", "kafka", "redis", "pyspark"
]


def extract_skills_from_text(text: str) -> str:
    text_lower = text.lower()
    found = [skill for skill in SKILL_KEYWORDS if skill in text_lower]
    return ", ".join(found)


def scrape_remotive(search_query: str = "data analyst") -> int:
    """
    Remotive.com public API — completely free, no auth needed.
    Returns remote tech jobs worldwide including India.
    """
    total_inserted = 0

    url = "https://remotive.com/api/remote-jobs"
    params = {"search": search_query, "limit": 100}

    print(f"Fetching from Remotive: '{search_query}'...")

    try:
        response = requests.get(url, params=params, timeout=15)
        print(f"Remotive status: {response.status_code}")

        if response.status_code != 200:
            print("Remotive failed")
            return 0

        data = response.json()
        jobs = data.get("jobs", [])
        print(f"Found {len(jobs)} jobs from Remotive")

        for job in jobs:
            try:
                description = job.get("description", "")
                # Strip HTML tags from description
                import re
                description_clean = re.sub(r'<[^>]+>', ' ', description)
                description_clean = re.sub(r'\s+', ' ', description_clean).strip()

                extracted_skills = extract_skills_from_text(
                    description_clean + " " + job.get("candidate_required_location", "")
                )

                job_data = {
                    "title": job.get("title", ""),
                    "company": job.get("company_name", ""),
                    "location": job.get("candidate_required_location", "Remote"),
                    "description": description_clean[:600],
                    "skills": extracted_skills,
                    "salary": job.get("salary", "Not disclosed"),
                    "experience": "",
                    "company_size": "",
                    "source": "remotive",
                    "posted_date": job.get("publication_date", ""),
                    "url": job.get("url", "")
                }

                if insert_job(job_data):
                    total_inserted += 1

            except Exception as e:
                print(f"Error parsing Remotive job: {e}")
                continue

    except Exception as e:
        print(f"Remotive error: {e}")

    print(f"Remotive done: {total_inserted} inserted")
    return total_inserted


def scrape_internshala(search_query: str = "data-science", pages: int = 3) -> int:
    """
    Internshala — best source for Indian internships.
    More scraper friendly than Naukri.
    """
    from bs4 import BeautifulSoup
    total_inserted = 0

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://internshala.com",
    }

    categories = [
        "data-science",
        "machine-learning",
        "python",
        "web-development",
        "artificial-intelligence"
    ]

    for category in categories:
        url = f"https://internshala.com/internships/{category}-internship"
        print(f"Scraping Internshala: {category}...")

        try:
            response = requests.get(url, headers=headers, timeout=15)
            print(f"Internshala status: {response.status_code}")

            if response.status_code != 200:
                continue

            soup = BeautifulSoup(response.content, "html.parser")

            cards = soup.find_all("div", class_="internship_meta")

            if not cards:
                cards = soup.find_all("div", attrs={"class": lambda x: x and "individual_internship" in str(x)})

            print(f"Found {len(cards)} cards for {category}")

            for card in cards:
                try:
                    full_text = card.get_text(separator=" ", strip=True)

                    link_tag = card.find("a", href=True)
                    job_url = ""
                    if link_tag:
                        href = link_tag.get("href", "")
                        job_url = f"https://internshala.com{href}" if href.startswith("/") else href
                        title = link_tag.get_text(strip=True)
                    else:
                        title = full_text[:60]

                    lines = [l.strip() for l in full_text.split("\n") if l.strip()]
                    company = lines[1] if len(lines) > 1 else ""

                    location = "India"
                    if "work from home" in full_text.lower():
                        location = "Remote"
                    elif "bangalore" in full_text.lower():
                        location = "Bangalore"
                    elif "delhi" in full_text.lower():
                        location = "Delhi"
                    elif "mumbai" in full_text.lower():
                        location = "Mumbai"

                    extracted_skills = extract_skills_from_text(full_text + " " + category)

                    if title and len(title) > 3:
                        job_data = {
                            "title": title,
                            "company": company,
                            "location": location,
                            "description": full_text[:500],
                            "skills": extracted_skills,
                            "salary": "Internship",
                            "experience": "Fresher",
                            "company_size": "",
                            "source": "internshala",
                            "posted_date": "",
                            "url": job_url
                        }

                        if insert_job(job_data):
                            total_inserted += 1

                except Exception as e:
                    print(f"Error parsing Internshala card: {e}")
                    continue

        except Exception as e:
            print(f"Internshala error ({category}): {e}")
            continue

    print(f"Internshala done: {total_inserted} inserted")
    return total_inserted


def scrape_github_jobs_archive() -> int:
    """
    Uses a public jobs dataset API — JSONPlaceholder style.
    Guaranteed to work, good for testing pipeline.
    """
    total_inserted = 0

    # Public job listings API — no auth, always works
    url = "https://api.adzuna.com/v1/api/jobs/in/search/1"

    # Using a free public dataset instead
    # jobicy.com has a free API
    url = "https://jobicy.com/api/v2/remote-jobs"
    params = {
        "count": 50,
        "tag": "data",
        "industry": "it"
    }

    print("Fetching from Jobicy API...")

    try:
        response = requests.get(url, params=params, timeout=15)
        print(f"Jobicy status: {response.status_code}")

        if response.status_code == 200:
            data = response.json()
            jobs = data.get("jobs", [])
            print(f"Found {len(jobs)} jobs from Jobicy")

            for job in jobs:
                try:
                    import re
                    description = re.sub(r'<[^>]+>', ' ', str(job.get("jobDescription", "")))
                    extracted_skills = extract_skills_from_text(
                        description + " " + str(job.get("jobIndustry", ""))
                    )

                    job_data = {
                        "title": job.get("jobTitle", ""),
                        "company": job.get("companyName", ""),
                        "location": job.get("jobGeo", "Remote"),
                        "description": description[:500],
                        "skills": extracted_skills,
                        "salary": job.get("annualSalaryMin", "Not disclosed"),
                        "experience": "",
                        "company_size": "",
                        "source": "jobicy",
                        "posted_date": job.get("pubDate", ""),
                        "url": job.get("url", "")
                    }

                    if insert_job(job_data):
                        total_inserted += 1

                except Exception as e:
                    continue

    except Exception as e:
        print(f"Jobicy error: {e}")

    print(f"Jobicy done: {total_inserted} inserted")
    return total_inserted


def scrape_naukri(search_query: str = "data analyst intern",
                  location: str = "India",
                  pages: int = 3) -> int:
    """
    Main function — tries multiple sources.
    """
    total = 0

    # Source 1 — Remotive (always works)
    total += scrape_remotive("data analyst")
    total += scrape_remotive("python developer")
    total += scrape_remotive("machine learning")

    # Source 2 — Jobicy (always works)
    total += scrape_github_jobs_archive()

    # Source 3 — Internshala (India specific)
    total += scrape_internshala()

    return total