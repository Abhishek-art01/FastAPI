import json
from typing import Generator
from sqlmodel import SQLModel, create_engine, Session

import os
import json
from sqlmodel import SQLModel, create_engine, Session

# --- 1. SETUP DATABASE URL ---


DATABASE_URL = os.environ.get("DATABASE_URL")

# If NOT on Render, try to load from local secrets.json
if not DATABASE_URL:
    try:
        with open("secrets.json") as f:
            all_secrets = json.load(f)
            # Switch this to "local" or "supabase" as needed for local testing
            secrets = all_secrets["supabase"] 
            
            DATABASE_URL = (
                f"postgresql://{secrets['DB_USER']}:{secrets['DB_PASSWORD']}"
                f"@{secrets['DB_HOST']}:{secrets.get('DB_PORT', 5432)}/{secrets['DB_NAME']}"
            )
    except FileNotFoundError:
        print("⚠️  WARNING: No secrets.json found and no DATABASE_URL set.")
        DATABASE_URL = "sqlite:///./test.db" # Fallback to a temporary local file

# --- 2. CREATE ENGINE ---
# Supabase/Postgres requires SSL. SQLite (fallback) does not.
connect_args = {}
if "postgresql" in DATABASE_URL:
    connect_args = {"sslmode": "require"}

engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True, connect_args=connect_args)



# --- 3. HELPER FUNCTIONS ---

def create_db_and_tables():
    """
    Creates tables based on imported SQLModel classes.
    Call this from main.py lifespan/startup.
    """
    SQLModel.metadata.create_all(engine)

def get_session() -> Generator[Session, None, None]:
    """
    Dependency to yield a database session per request.
    """
    with Session(engine) as session:
        yield session