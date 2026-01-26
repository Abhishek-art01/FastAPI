import pandas as pd
import numpy as np
import pdfplumber
import io
import re
from sqlmodel import Session, select, col
import xlrd
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
import traceback

# ==========================================
# 0. CONFIGURATION & GLOBAL HELPERS
# ==========================================

# 1. The Headers for Excel Output (Friendly Names)
MANDATORY_HEADERS = [
    'Shift Date', 'Trip ID', 'Employee ID', 'Gender', 'EMP_CATEGORY', 'Employee Name', 
    'Shift Time', 'Pickup Time', 'Drop Time', 'Trip Direction', 'Cab Reg No', 
    'Cab Type', 'Vendor', 'Office', 'Airport Name', 'Landmark', 'Address', 
    'Flight Number', 'Flight Category', 'Flight Route', 'Flight Type', 
    'Trip Date', 'MiS Remark', 'In App/ Extra', 'UNA2', 'UNA', 
    'Route Status', 'Clubbing Status', 'GPS TIME', 'GPS REMARK',
    'passenger_mobile', 'driver_name', 'DRIVER_MOBILE' 
]

# 2. Map Friendly Headers -> SQLModel Fields (snake_case)
MANDATORY_DB_MAP = {
    'Shift Date': 'shift_date', 'Trip ID': 'trip_id', 'Employee ID': 'employee_id', 
    'Gender': 'gender', 'EMP_CATEGORY': 'emp_category', 'Employee Name': 'employee_name', 
    'Shift Time': 'shift_time', 'Pickup Time': 'pickup_time', 'Drop Time': 'drop_time', 
    'Trip Direction': 'trip_direction', 'Cab Reg No': 'cab_reg_no', 'Cab Type': 'cab_type', 
    'Vendor': 'vendor', 'Office': 'office', 'Airport Name': 'airport_name', 
    'Landmark': 'landmark', 'Address': 'address', 'Flight Number': 'flight_number', 
    'Flight Category': 'flight_category', 'Flight Route': 'flight_route', 
    'Flight Type': 'flight_type', 'Trip Date': 'trip_date', 'MiS Remark': 'mis_remark', 
    'In App/ Extra': 'in_app_extra', 'UNA2': 'una2', 'UNA': 'una', 
    'Route Status': 'route_status', 'Clubbing Status': 'clubbing_status', 
    'GPS TIME': 'gps_time', 'GPS REMARK': 'gps_remark',
    'passenger_mobile': 'passenger_mobile', 'driver_name': 'driver_name', 'DRIVER_MOBILE': 'driver_mobile'
}

def clean_columns(columns):
    cleaned = (
        columns
        .str.replace(r"\n", " ", regex=True)        # remove line breaks
        .str.replace(r"\t", " ", regex=True)        # remove tabs
        .str.replace(r"\s+", " ", regex=True)       # normalize spaces
        .str.strip()                                # trim edges
        .str.lower()                                # lowercase
        .str.replace(r"[^\w\s]", "", regex=True)    # remove special chars
        .str.replace(" ", "_")                      # snake_case
    )
    return cleaned

import pandas as pd

def clean_address(series: pd.Series) -> pd.Series:
    """
    Cleans address text exactly like:
    =UPPER(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(A2,"-"," "),","," "),"/"," "))
    """
    return (
        series
        .astype(str)
        .str.replace("-", " ", regex=False)
        .str.replace(",", " ", regex=False)
        .str.replace("/", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)  # normalize spaces
        .str.strip()
        .str.upper()
    )


def standardize_dataframe(df):
    """
    Standardizes DataFrame to match SQLModel definitions:
    1. Generates 'unique_id' (Composite Key).
    2. Ensures ALL mapped columns exist (fills missing with "").
    3. Returns DataFrame with [Mandatory Columns] + [Any Extra Columns].
    """
    if df is None or df.empty:
        return None

    # 1. Generate Unique ID
    if 'trip_id' in df.columns and 'employee_id' in df.columns:
        df['unique_id'] = df['trip_id'].astype(str).str.strip() + df['employee_id'].astype(str).str.strip()
        # Filter invalid IDs
        df = df[
            df["unique_id"].notna() & 
            (df["unique_id"] != "") & 
            (~df["unique_id"].str.contains("nan|None", case=False))
        ]
    else:
        return None # Critical missing data

    # 2. Sync Columns with SQLModel (Fill Missing)
    target_cols = list(MANDATORY_DB_MAP.values())
    for col in target_cols:
        if col not in df.columns:
            df[col] = ""

    # 3. Populate specific logic columns
    df['una2'] = df['unique_id'] # As requested
    # Optional: populate 'una' if needed, or leave blank

    # 4. Organize Columns: Mandatory -> Extra -> unique_id
    current_cols = df.columns.tolist()
    reserved_cols = set(target_cols) | {'unique_id'}
    extra_cols = [c for c in current_cols if c not in reserved_cols]
    
    final_order = target_cols + extra_cols + ['unique_id']
    return df[final_order]

def get_xls_style_data(book, xf_index):
    """ Helper to extract style (Hex Color, Bold) from xlrd book. """
    try:
        xf = book.xf_list[xf_index]
        font = book.font_list[xf.font_index]
        
        def get_hex(idx):
            if idx is not None and idx < 64:
                rgb = book.colour_map.get(idx)
                if rgb and rgb != (0,0,0) and rgb != (255, 255, 255):
                    return "{:02x}{:02x}{:02x}".format(*rgb).upper()
            return None

        bg_hex = get_hex(xf.background.pattern_colour_index)
        font_hex = get_hex(font.colour_index)
        return bg_hex, font_hex, bool(font.bold)
    except:
        return None, None, False

def create_styled_excel(df, filename_prefix="Cleaned"):
    """ Generates Excel with consistent formatting. """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # Invert map to get Friendly Headers
        inv_map = {v: k for k, v in MANDATORY_DB_MAP.items()}
        df_export = df.rename(columns=inv_map)
        
        # Order: Mandatory Headers -> Extra Headers
        export_cols = [h for h in MANDATORY_HEADERS if h in df_export.columns]
        remaining = [c for c in df_export.columns if c not in MANDATORY_HEADERS and c != 'unique_id']
        export_cols += remaining
        
        df_export = df_export[export_cols]
        
        df_export.to_excel(writer, index=False, sheet_name='Raw_Data')
        
        # Apply Styles
        workbook = writer.book
        worksheet = writer.sheets['Raw_Data']
        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#0070C0', 'font_color': 'white'})
        
        for i, col in enumerate(df_export.columns):
            worksheet.write(0, i, col, header_fmt)
            worksheet.set_column(i, i, 15)
            
    output.seek(0)
    return df, output, f"{filename_prefix}.xlsx"

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
