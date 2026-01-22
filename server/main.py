import os
import io
from sqlalchemy import text
import pandas as pd
from pathlib import Path
from contextlib import asynccontextmanager
from typing import List, Optional
from datetime import datetime

from fastapi import FastAPI, Depends, Request, Form, Response, UploadFile, File, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, FileResponse, JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from pydantic import BaseModel

# SQLModel & Admin
from sqlmodel import select, Session, desc, col, update, SQLModel
from sqladmin import Admin, ModelView
from sqladmin.authentication import AuthenticationBackend

# --- INTERNAL IMPORTS ---
from .auth import verify_password, get_password_hash
from .database import create_db_and_tables, get_session, engine
from .models import User, ClientData, RawTripData, OperationData, TripData, T3AddressLocality, T3LocalityZone, T3ZoneKm
from .cleaner import process_client_data, process_raw_data, process_operation_data

# --- 1. CONFIGURATION & PATHS ---
BASE_DIR = Path(__file__).resolve().parent
CLIENT_DIR = BASE_DIR.parent / "client"
COMPONENTS_DIR = CLIENT_DIR / "Components"

DIRS = {
    "home": CLIENT_DIR / "HomePage",
    "login": CLIENT_DIR / "LoginPage",
    "cleaner": CLIENT_DIR / "DataCleaner",
    "gps": CLIENT_DIR / "GPSCorner",
    "operation-manager": CLIENT_DIR / "OperationManager",
    "locality": CLIENT_DIR / "LocalityCorner",
    "components": CLIENT_DIR / "Components"
}


# --- 2. LIFESPAN (Startup & Sequence Fix) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    
    # Auto-fix sequence for Address Table (Prevents "Key (id)=(x) already exists" error)
    try:
        with Session(engine) as session:
            session.exec(text("SELECT setval(pg_get_serial_sequence('t3_address_locality', 'id'), coalesce(max(id),0) + 1, false) FROM t3_address_locality;"))
            session.commit()
            print("✅ Address Table Sequence Sync Completed.")
    except Exception:
        pass 
        
    yield

app = FastAPI(lifespan=lifespan)

# --- 3. MIDDLEWARE ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://localhost:5000",
        "https://aitarowdatacleaner.onrender.com"
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Authorization", "Content-Type"],
)


on_render = os.environ.get("RENDER") is not None
app.add_middleware(
    SessionMiddleware,
    secret_key="super_secret_static_key",
    # secret_key=os.environ["SESSION_SECRET"]
    max_age=3600, 
    https_only=on_render, 
    same_site="lax"
)

# --- 4. STATIC FILES & TEMPLATES ---
app.mount("/home-static", StaticFiles(directory=DIRS["home"]), name="home_static")
app.mount("/login-static", StaticFiles(directory=DIRS["login"]), name="login_static")
app.mount("/cleaner-static", StaticFiles(directory=DIRS["cleaner"]), name="cleaner_static")
app.mount("/gps-corner-static", StaticFiles(directory=DIRS["gps"]), name="gps_static")
app.mount("/locality-static", StaticFiles(directory=DIRS["locality"]), name="locality_static")
app.mount("/operation-manager-static", StaticFiles(directory=DIRS["operation-manager"]), name="operation-manager_static")
app.mount("/components-static", StaticFiles(directory=DIRS["components"]), name="components_static")

templates = {
    "home": Jinja2Templates(directory=DIRS["home"]),
    "login": Jinja2Templates(directory=DIRS["login"]),
    "cleaner": Jinja2Templates(directory=DIRS["cleaner"]),
    "gps": Jinja2Templates(directory=DIRS["gps"]),
    "locality": Jinja2Templates(directory=DIRS["locality"]),
    "operation-manager": Jinja2Templates(directory=DIRS["operation-manager"]),
    "components": Jinja2Templates(directory=DIRS["components"]),
}

# --- 5. AUTHENTICATION BACKEND ---
class AdminAuth(AuthenticationBackend):
    async def login(self, request: Request) -> bool:
        form = await request.form()
        username, password = form.get("username"), form.get("password")
        allowed_users = ["admin", "chickenman"]
        if username not in allowed_users:
            return False

        with Session(engine) as session:
            user = session.exec(select(User).where(User.username == username)).first()
            if user and verify_password(password, user.password_hash):
                request.session.update({"user": user.username})
                return True
        return False

    async def logout(self, request: Request) -> bool:
        request.session.clear()
        return True

    async def authenticate(self, request: Request) -> bool:
        return request.session.get("user") in ["admin", "chickenman"]

# --- 6. ADMIN VIEWS ---
class UserAdmin(ModelView, model=User):
    column_list = [User.id, User.username]
    form_args = dict(password_hash=dict(label="Password (Leave blank to keep)"))

    async def on_model_change(self, data, model, is_created, request):
        password = data.get("password_hash")
        if not password:
            if not is_created: del data["password_hash"]
        elif not (len(password) == 60 and password.startswith("$")):
            hashed = get_password_hash(password)
            model.password_hash = hashed
            data["password_hash"] = hashed

class TripDataAdmin(ModelView, model=TripData): column_list = [TripData.shift_date, TripData.unique_id, TripData.employee_name, TripData.cab_reg_no, TripData.trip_direction]
class ClientDataAdmin(ModelView, model=ClientData): column_list = [ClientData.id, ClientData.unique_id, ClientData.employee_name]
class RawTripDataAdmin(ModelView, model=RawTripData): column_list = [RawTripData.id, RawTripData.unique_id, RawTripData.trip_date]
class OperationDataAdmin(ModelView, model=OperationData): column_list = [OperationData.id, OperationData.unique_id]
class AddressLocalityAdmin(ModelView, model=T3AddressLocality):
    name = "Address Master"
    icon = "fa-solid fa-map-pin"
    column_list = [T3AddressLocality.id, T3AddressLocality.address, T3AddressLocality.locality]
    column_searchable_list = [T3AddressLocality.address, T3AddressLocality.locality]


admin = Admin(app, engine, authentication_backend=AdminAuth(secret_key="super_secret_static_key"))
admin.add_view(UserAdmin)
admin.add_view(TripDataAdmin)
admin.add_view(ClientDataAdmin)
admin.add_view(RawTripDataAdmin)
admin.add_view(OperationDataAdmin)
admin.add_view(AddressLocalityAdmin)

# --- 6. HELPER FUNCTIONS ---
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



# --- 7. PAGE ROUTES ---
@app.get("/")
async def read_root(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    return templates["home"].TemplateResponse("homepage.html", {"request": request, "user": user})

@app.get("/login")
async def login_page(request: Request):
    if request.session.get("user"):
        return RedirectResponse(url="/", status_code=303)
    return templates["login"].TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login_user(request: Request, username: str = Form(...), password: str = Form(...), session: Session = Depends(get_session)):
    user = session.exec(select(User).where(User.username == username)).first()
    if user and verify_password(password, user.password_hash):
        request.session["user"] = user.username
        return RedirectResponse(url="/", status_code=303)
    return templates["login"].TemplateResponse("login.html", {"request": request, "error": "Invalid credentials"})

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/")

@app.get("/cleaner")
async def cleaner_page(request: Request):
    if not request.session.get("user"): return RedirectResponse(url="/login", status_code=303)
    return templates["cleaner"].TemplateResponse("Datacleaner.html", {"request": request, "user": request.session.get("user")})

@app.get("/gps-corner")
async def gps_page(request: Request):
    if not request.session.get("user"): return RedirectResponse(url="/login", status_code=303)
    return templates["gps"].TemplateResponse("gps_corner.html", {"request": request, "user": request.session.get("user")})

@app.get("/operation-manager")
async def operation_manager_page(request: Request):
    if not request.session.get("user"): return RedirectResponse(url="/login", status_code=303)
    return templates["operation-manager"].TemplateResponse("operation_manager.html", {"request": request})

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204) 
# ==========================================
# 🚀 DATA CLEANER API 
# ==========================================
@app.post("/clean-data")
async def clean_data(
    files: List[UploadFile] = File(...),
    cleanerType: str = Form(...),
    session: Session = Depends(get_session)
):
    try:
        print(f"🚀 Processing {len(files)} files with mode: {cleanerType}")
        df_result = None
        excel_output = None
        filename = "output.xlsx"
        rows_saved = 0
        new_addresses = 0


        # --- A. CLIENT DATA ---
        if cleanerType == "client":
            content = await files[0].read()
            df_result, excel_output, filename = process_client_data(content)
            rows_saved = bulk_save_unique(session, ClientData, df_result)
            
            # 🔥 SYNC ADDRESSES
            if df_result is not None:
                new_addresses = sync_addresses_to_t3(session, df_result)
            
            if df_result is not None and not df_result.empty and "unique_id" in df_result.columns:
                incoming_ids = df_result["unique_id"].dropna().unique().tolist()
                existing_ids = set(session.exec(select(ClientData.unique_id).where(col(ClientData.unique_id).in_(incoming_ids))).all())
                new_rows = df_result[~df_result["unique_id"].isin(existing_ids)]
                if not new_rows.empty:
                    records = [ClientData(**row.to_dict()) for _, row in new_rows.iterrows()]
                    session.add_all(records)
                    session.commit()

        # --- B. RAW DATA ---
        elif cleanerType == "raw":
            file_data = []
            for f in files:
                content = await f.read()
                file_data.append((f.filename, content))
            df_result, excel_output, filename = process_raw_data(file_data)
            rows_saved = bulk_save_unique(session, RawTripData, df_result)
            
            # 🔥 SYNC ADDRESSES (Usually raw data has addresses too)
            if df_result is not None:
                new_addresses = sync_addresses_to_t3(session, df_result)
            
            if df_result is not None and not df_result.empty and "unique_id" in df_result.columns:
                incoming_ids = df_result["unique_id"].dropna().unique().tolist()
                existing_ids = set(session.exec(select(RawTripData.unique_id).where(col(RawTripData.unique_id).in_(incoming_ids))).all())
                new_rows = df_result[~df_result["unique_id"].isin(existing_ids)]
                if not new_rows.empty:
                    records = [RawTripData(**row.to_dict()) for _, row in new_rows.iterrows()]
                    session.add_all(records)
                    session.commit()

        elif cleanerType == "operation":
            file_data = []
            for f in files:
                content = await f.read()
                file_data.append((f.filename, content))
            df_result, excel_output, filename = process_operation_data(file_data)

        if excel_output is None:
            return Response("Error processing data", status_code=400)

        generated_dir = DIRS["cleaner"] / "generated"
        os.makedirs(generated_dir, exist_ok=True)
        save_path = generated_dir / filename
        with open(save_path, "wb") as f:
            f.write(excel_output.read())

        row_count = len(df_result) if df_result is not None else "Formatting Only"
        return {
            "status": "success",
            "file_url": filename,
            "rows_processed": row_count,
            "db_rows_added": rows_saved
        }

    except Exception as e:
        print(f"❌ Server Error: {e}")
        return Response(f"Internal Error: {e}", status_code=500)



# ==========================================
# 9. GPS CORNER API 
# ==========================================

# 1. THE GET ROUTE
@app.get("/api/gps_trips", response_model=List[TripData])
def read_gps_trips(
    date: str = None, 
    vehicle: str = None, 
    trip_direction: str = None,
    session: Session = Depends(get_session)
):
    # Base query
    query = select(TripData)
    
    # FILTER 1: Exclude 'Pay' status
    if hasattr(TripData, "clubbing_status"):
        query = query.where(col(TripData.clubbing_status).ilike("%not pay%"))

    # FILTER 2: Date 
    if date:
        try:
            # Parse HTML 'YYYY-MM-DD' -> Convert to DB 'DD-MM-YYYY'
            date_obj = datetime.strptime(date, "%Y-%m-%d")
            formatted_date = date_obj.strftime("%d-%m-%Y")
            
            print(f"🔍 Searching for date: {formatted_date}") 
            query = query.where(TripData.shift_date.contains(formatted_date))
        except ValueError:
            # Fallback for simple string match
            query = query.where(TripData.shift_date.contains(date))

    # FILTER 3: Vehicle
    if vehicle:
        query = query.where(TripData.cab_reg_no.contains(vehicle))

    results = session.exec(query).all()
    return results

# 2. THE UPDATE ROUTE (Using Unique ID)
# ---------------------------------------------------------
# ROBUST UPDATE ROUTE (Fixes Key Mismatch & Scientific Notation)
# ---------------------------------------------------------
@app.post("/api/update_gps/{unique_id}")
def update_gps_data(unique_id: str, payload: dict, session: Session = Depends(get_session)):
    print(f"🔥 DEBUG: Request for Unique ID: {unique_id}")
    print(f"📦 DEBUG: Data Received: {payload}")

    # 1. Find the trip
    # We strip whitespace just in case
    clean_id = str(unique_id).strip()
    statement = select(TripData).where(col(TripData.unique_id) == clean_id)
    trip = session.exec(statement).first()
    
    if not trip:
        print(f"❌ DEBUG: Unique ID '{clean_id}' not found.")
        raise HTTPException(status_code=404, detail="Trip not found")

    # 2. UPDATE FIELDS (Checking BOTH possible key names)
    
    # Start Location
    if "journey_start_location" in payload:
        trip.journey_start_location = payload["journey_start_location"]
    elif "journey_start" in payload:
        trip.journey_start_location = payload["journey_start"]
    elif "start" in payload:
        trip.journey_start_location = payload["start"]

    # End Location
    if "journey_end_location" in payload:
        trip.journey_end_location = payload["journey_end_location"]
    elif "journey_end" in payload:
        trip.journey_end_location = payload["journey_end"]
    elif "end" in payload:
        trip.journey_end_location = payload["end"]

    # Remarks
    if "gps_remark" in payload:
        trip.gps_remark = payload["gps_remark"]
    elif "remark" in payload:
        trip.gps_remark = payload["remark"]

    # GPS Time
    if "gps_time" in payload:
        trip.gps_time = payload["gps_time"]

    # 3. Save to DB
    session.add(trip)
    session.commit()
    session.refresh(trip)
    
    print(f"✅ DEBUG: Saved to DB! Start={trip.journey_start_location}, End={trip.journey_end_location}")
    return {"status": "success", "data": trip}
# ==========================================
# 4. UNIVERSAL DOWNLOAD ENDPOINTS
# ==========================================
@app.get("/download/{filename}")
async def download_file(filename: str, request: Request):
    if not request.session.get("user"):
        return Response("Unauthorized", status_code=401)
    
    file_path = DIRS["cleaner"] / "generated" / filename
    if not file_path.exists():
        return Response("File not found", status_code=404)
        
    return FileResponse(
        path=file_path, 
        filename=filename, 
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
@app.get("/api/{table_type}/download")
def download_specific_table(table_type: str, session: Session = Depends(get_session)):
    model_map = {
        "operation": OperationData,
        "client": ClientData,
        "raw": RawTripData,
        "trip_data": TripData
    }
    
    if table_type not in model_map:
        return {"status": "error", "message": "Invalid table type selected."}
    
    model_class = model_map[table_type]
    statement = select(model_class)
    results = session.exec(statement).all()
    
    if not results:
        return {"status": "error", "message": f"No data found in {table_type} table."}
    
    data = [row.model_dump() for row in results]
    df = pd.DataFrame(data)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Report')
    output.seek(0)
    
    filename = f"{table_type.capitalize()}_Export.xlsx"
    headers = {'Content-Disposition': f'attachment; filename="{filename}"'}
    return StreamingResponse(
        output, 
        headers=headers, 
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

@app.post("/api/operation/upload")
async def upload_operation_data(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))
        
        save_path = CLIENT_DIR / "OperationManager" / "processed_db_mock.csv"
        # Ensure dir exists
        os.makedirs(save_path.parent, exist_ok=True)
        df.to_csv(save_path, index=False)

        return JSONResponse(
            content={
                "status": "success", 
                "message": f"Successfully processed {len(df)} rows and updated Database."
            }
        )
    except Exception as e:
        print(f"Error processing file: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)}
        )

# ==========================================
# 📍 LOCALITY MANAGER API (Complete)
# ==========================================

# 1. PAGE ROUTE
@app.get("/locality-manager")
async def locality_manager_page(request: Request):
    if not request.session.get("user"): return RedirectResponse(url="/login", status_code=303)
    return templates["locality"].TemplateResponse("Localitycorner.html", {"request": request, "user": request.session.get("user")})

# --- Pydantic Schemas ---
class LocalityMappingSchema(BaseModel):
    address_id: int
    locality_name: str 

class BulkMappingSchema(BaseModel):
    address_ids: List[int]
    locality_name: str

class NewMasterSchema(BaseModel):
    locality_name: str
    zone_name: str

# 2. API: Get Dropdown Data (One-line fetch & format)
@app.get("/api/dropdown-localities/")
def get_master_localities(session: Session = Depends(get_session)):
    return [
        {**loc.model_dump(), "billing_km": km or "-"} 
        for loc, km in session.exec(
            select(T3LocalityZone, T3ZoneKm.km)
            .join(T3ZoneKm, T3LocalityZone.zone == T3ZoneKm.zone, isouter=True)
            .order_by(T3LocalityZone.locality)
        ).all()
    ]

# 3. API: View All / Pagination
@app.get("/api/localities/")
def get_address_table(page: int = 1, search: str = "", session: Session = Depends(get_session)):
    limit = 20
    offset = (page - 1) * limit
    
    # Correct JOIN for your Schema: Address -> LocalityZone -> ZoneKm
    query = select(T3AddressLocality, T3LocalityZone, T3ZoneKm)\
        .join(T3LocalityZone, T3AddressLocality.locality == T3LocalityZone.locality, isouter=True)\
        .join(T3ZoneKm, T3LocalityZone.zone == T3ZoneKm.zone, isouter=True)\
        .order_by(desc(T3AddressLocality.id))
    
    if search:
        query = query.where(T3AddressLocality.address.contains(search))
    
    total_records = len(session.exec(select(T3AddressLocality).where(T3AddressLocality.address.contains(search))).all())
    pending_count = len(session.exec(select(T3AddressLocality).where(col(T3AddressLocality.locality).is_(None))).all())
    
    results = session.exec(query.offset(offset).limit(limit)).all()
    
    data = []
    for address_row, locality_row, zone_km_row in results:
        data.append({
            "id": address_row.id,
            "address": address_row.address,
            "locality_id": address_row.locality, # String is the ID here
            "locality": address_row.locality,
            "zone": locality_row.zone if locality_row else None, 
            "km": zone_km_row.km if zone_km_row else 0, # Fetch KM from joined table
            "status": "Done" if address_row.locality else "Pending"
        })
        
    return {
        "results": data,
        "pagination": {"total_pages": (total_records // limit) + 1},
        "global_pending": pending_count
    }

# 4. API: Get Next Pending Item
@app.get("/api/next-pending/")
def get_next_pending(session: Session = Depends(get_session)):
    row = session.exec(select(T3AddressLocality).where(col(T3AddressLocality.locality).is_(None)).limit(1)).first()
    if not row:
        return {"found": False}
    return {"found": True, "data": row}

# 5. API: Save Single Mapping
@app.post("/api/save-mapping/")
def save_mapping(data: LocalityMappingSchema, session: Session = Depends(get_session)):
    row = session.get(T3AddressLocality, data.address_id)
    if not row:
        return JSONResponse({"success": False, "error": "Address not found"}, status_code=404)
    
    # Update Relation
    row.locality = data.locality_name
    
    # Update Cache Fields (Zone/KM) automatically
    locality_info = session.exec(
        select(T3LocalityZone, T3ZoneKm)
        .join(T3ZoneKm, T3LocalityZone.zone == T3ZoneKm.zone, isouter=True)
        .where(T3LocalityZone.locality == data.locality_name)
    ).first()

    if locality_info:
        loc_row, km_row = locality_info
        row.zone = loc_row.zone
        row.km = km_row.km if km_row else None

    session.add(row)
    session.commit()
    return {"success": True}

# 6. API: Search Pending
@app.get("/api/search-pending/")
def search_pending(q: str = "", page: int = 1, session: Session = Depends(get_session)):
    limit = 50
    offset = (page - 1) * limit
    
    query = select(T3AddressLocality).where(col(T3AddressLocality.locality).is_(None))
    if q:
        query = query.where(T3AddressLocality.address.contains(q))
        
    total = len(session.exec(query).all())
    results = session.exec(query.offset(offset).limit(limit)).all()
    
    return {
        "results": results,
        "pagination": {"total_records": total}
    }

# 7. API: Bulk Save
@app.post("/api/bulk-save/")
def bulk_save(data: BulkMappingSchema, session: Session = Depends(get_session)):
    cache_values = {"locality": data.locality_name}
    
    locality_info = session.exec(
        select(T3LocalityZone, T3ZoneKm)
        .join(T3ZoneKm, T3LocalityZone.zone == T3ZoneKm.zone, isouter=True)
        .where(T3LocalityZone.locality == data.locality_name)
    ).first()

    if locality_info:
        loc_row, km_row = locality_info
        cache_values["zone"] = loc_row.zone
        cache_values["km"] = km_row.km if km_row else None

    statement = (
        update(T3AddressLocality)
        .where(col(T3AddressLocality.id).in_(data.address_ids))
        .values(**cache_values)
    )
    result = session.exec(statement)
    session.commit()
    return {"success": True, "count": result.rowcount}

# 8. API: Add New Master Locality
@app.post("/api/add-master-locality/")
def add_master(data: NewMasterSchema, session: Session = Depends(get_session)):
    try:
        # Auto-create Zone if missing to avoid FK error
        if not session.get(T3ZoneKm, data.zone_name):
             session.add(T3ZoneKm(zone=data.zone_name, km="0")) 
        
        new_loc = T3LocalityZone(locality=data.locality_name, zone=data.zone_name)
        session.add(new_loc)
        session.commit()
        return {"success": True}
    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)}, status_code=400)