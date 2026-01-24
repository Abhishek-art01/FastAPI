import pandas as pd
from sqlmodel import Session, select, col
from sqlalchemy import text
from ..models import T3AddressLocality

# --- HELPER FUNCTIONS ---
def bulk_save_unique(session: Session, model_class, df: pd.DataFrame, unique_col: str = "unique_id") -> int:
    """Helper to insert only new rows into database based on a unique column."""
    if df is None or df.empty or unique_col not in df.columns:
        return 0
    
    incoming_ids = df[unique_col].dropna().unique().tolist()
    if not incoming_ids: return 0

    existing_ids = set(session.exec(select(getattr(model_class, unique_col)).where(col(getattr(model_class, unique_col)).in_(incoming_ids))).all())
    new_rows = df[~df[unique_col].isin(existing_ids)]
    
    if not new_rows.empty:
        records = [model_class(**row.where(pd.notnull(row), None).to_dict()) for _, row in new_rows.iterrows()]
        session.add_all(records)
        session.commit()
        return len(new_rows)
    return 0

def sync_addresses_to_t3(session: Session, df: pd.DataFrame) -> int:
    """
    Extracts unique addresses from the uploaded dataframe and adds NEW ones
    to the t3_address_locality table. Handles duplicates and ID conflicts.
    """
    # 1. Identify Address Column
    address_col = None
    possible_names = ["address", "Address", "employee_address", "Employee Address", "pickup_location", "drop_location"]
    
    for col_name in possible_names:
        if col_name in df.columns:
            address_col = col_name
            break
    
    if not address_col:
        print("⚠️ T3 Sync: No address column found in uploaded file.")
        return 0

    # 2. Extract Unique Addresses from File
    file_addresses = set(
        df[address_col]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
    )
    file_addresses.discard("") # Remove empty strings

    if not file_addresses:
        return 0

    # 3. Find Addresses ALREADY in Database
    # Fetch all existing addresses to compare against
    # Use chunking if you have millions of rows, but for thousands, this is fine.
    existing_db_addresses = set(session.exec(select(T3AddressLocality.address)).all())
    
    # 4. Filter New Addresses
    new_addresses_list = list(file_addresses - existing_db_addresses)

    if not new_addresses_list:
        print("✅ T3 Sync: All addresses already exist.")
        return 0

    print(f"📍 T3 Sync: Found {len(new_addresses_list)} NEW addresses. Inserting...")

    # 5. Insert One-by-One to Isolate Failures (Safer for debugging conflicts)
    # Or Bulk Insert if confident. Let's try a safer Bulk approach.
    
    records = [T3AddressLocality(address=addr, locality=None) for addr in new_addresses_list]
    
    try:
        session.add_all(records)
        session.commit()
        return len(records)
    except Exception as e:
        session.rollback()
        print(f"❌ T3 Sync Error: {e}")
        
        # AUTO-FIX: Attempt to reset the ID sequence if it's a Primary Key error
        if "t3_address_locality_pkey" in str(e) or "UniqueViolation" in str(e):
            print("🔧 Attempting to fix ID sequence...")
            try:
                # This SQL command resets the ID counter to the max ID + 1
                session.exec(text("SELECT setval(pg_get_serial_sequence('t3_address_locality', 'id'), coalesce(max(id),0) + 1, false) FROM t3_address_locality;"))
                session.commit()
                
                # Retry Insert
                print("🔄 Retrying insert after sequence fix...")
                session.add_all(records)
                session.commit()
                return len(records)
            except Exception as retry_e:
                print(f"❌ Retry Failed: {retry_e}")
                return 0
        return 0
