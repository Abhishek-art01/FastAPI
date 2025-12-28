import json
from typing import Generator
from sqlmodel import SQLModel, create_engine, Session

# --- 1. SETUP DATABASE URL ---
with open("secrets.json") as f:
    all_secrets = json.load(f)

# 👇 CHOOSE HERE: Change this string to "local" or "supabase" to switch!
CURRENT_ENV = "supabase" 

secrets = all_secrets[CURRENT_ENV]

# The rest of your code stays exactly the same...
DATABASE_URL = (
    f"postgresql://{secrets['DB_USER']}:{secrets['DB_PASSWORD']}"
    f"@{secrets['DB_HOST']}:{secrets.get('DB_PORT', 5432)}/{secrets['DB_NAME']}"
)

# --- 2. CREATE ENGINE ---
# pool_pre_ping=True is CRITICAL for Postgres. 
# It checks if the connection is alive before using it, preventing "Closed connection" errors.
engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)

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