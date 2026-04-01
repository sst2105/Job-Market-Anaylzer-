import sqlite3
import pandas as pd
from datetime import datetime
import os

# Resolve paths from this file so the DB location is stable regardless of cwd.
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(_PROJECT_ROOT, "data", "jobs.db")

def init_db():
    """
    Create the jobs database and tables if they don't exist.
    Run this once at startup.
    """
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            company TEXT,
            location TEXT,
            description TEXT,
            skills TEXT,
            salary TEXT,
            experience TEXT,
            company_size TEXT,
            source TEXT,
            posted_date TEXT,
            scraped_at TEXT,
            url TEXT UNIQUE
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS skills_extracted (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER,
            skill TEXT,
            FOREIGN KEY (job_id) REFERENCES jobs(id)
        )
    """)
    
    conn.commit()
    conn.close()
    print("Database initialized")


def insert_job(job_data: dict) -> bool:
    """
    Insert a single job into the database.
    Returns True if inserted, False if duplicate (same URL).
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT OR IGNORE INTO jobs 
            (title, company, location, description, skills, salary, 
             experience, company_size, source, posted_date, scraped_at, url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job_data.get("title", ""),
            job_data.get("company", ""),
            job_data.get("location", ""),
            job_data.get("description", ""),
            job_data.get("skills", ""),
            job_data.get("salary", ""),
            job_data.get("experience", ""),
            job_data.get("company_size", ""),
            job_data.get("source", ""),
            job_data.get("posted_date", ""),
            datetime.now().isoformat(),
            job_data.get("url", "")
        ))
        
        inserted = cursor.rowcount > 0
        conn.commit()
        return inserted
        
    except Exception as e:
        print(f"DB insert error: {e}")
        return False
    finally:
        conn.close()


def get_all_jobs() -> pd.DataFrame:
    """
    Return all jobs as a pandas DataFrame.
    This is what the analysis and dashboard use.
    """
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM jobs", conn)
    conn.close()
    return df


def get_job_count() -> int:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM jobs")
    count = cursor.fetchone()[0]
    conn.close()
    return count


if __name__ == "__main__":
    init_db()
