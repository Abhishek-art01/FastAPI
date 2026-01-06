from fastapi import FastAPI, Depends, Request, Form, Response, UploadFile, File
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
from sqlmodel import select, Session, desc
from sqladmin import Admin, ModelView
from sqladmin.authentication import AuthenticationBackend
from pathlib import Path
from contextlib import asynccontextmanager
import os
from fastapi.middleware.cors import CORSMiddleware
import io
import zipfile
from fastapi.responses import FileResponse # 👈 Add this import at top if missing
from .models import TripData 
from pydantic import BaseModel
import math
from typing import Optional


# --- IMPORTS ---
from .auth import verify_password, get_password_hash
from .database import create_db_and_tables, get_session, engine
from .models import BillingRecord, User
from .cleaner import process_dataframe, to_excel_billing, to_excel_operations # 👈 Import from new file


# --- SETUP PATHS ---
BASE_DIR = Path(__file__).resolve().parent
CLIENT_DIR = BASE_DIR.parent / "client" / "HomePage"
LOGIN_DIR = BASE_DIR.parent / "client" / "LoginPage"
CLEANER_DIR = BASE_DIR.parent / "client" / "DataCleaner"
GPS_DIR = BASE_DIR.parent / "client" / "GPSCorner"


# 3. Setup Templates

@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield

app = FastAPI(lifespan=lifespan)

# --- 1. MIDDLEWARE (The Fix for Render) ---

# 1. CORS Middleware (For Frontend Communication)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)

# 2. Session Middleware (For Cookies)
# We check if we are on Render by looking for the 'RENDER' environment variable.
# If on Render -> Secure Cookies (https_only=True)
# If on Laptop -> Normal Cookies (https_only=False)
on_render = os.environ.get("RENDER") is not None

app.add_middleware(
    SessionMiddleware,
    secret_key="super_secret_static_key",
    max_age=3600,       # 1 hour
    https_only=on_render, # 👈 AUTOMATIC SWITCH: True on Server, False on Laptop
    same_site="lax"
)

# --- 2. TEMPLATES & STATIC ---
app.mount("/static", StaticFiles(directory=CLIENT_DIR), name="static")
app.mount("/login-static", StaticFiles(directory=LOGIN_DIR), name="login_static")
app.mount("/cleaner-static", StaticFiles(directory=CLEANER_DIR), name="cleaner_static")
app.mount("/gps-static", StaticFiles(directory=GPS_DIR), name="gps_static")

templates = Jinja2Templates(directory=CLIENT_DIR)
login_templates = Jinja2Templates(directory=LOGIN_DIR)
cleaner_templates = Jinja2Templates(directory=CLEANER_DIR)
gps_templates = Jinja2Templates(directory=GPS_DIR)

#Some Helper Functions

def safe_str(value):
    """Converts value to string, but returns None if empty or 'nan'."""
    if value is None or (isinstance(value, float) and math.isnan(value)) or str(value).strip() == "":
        return None
    return str(value).strip()

def safe_float(value):
    """Converts value to float, returns None if empty."""
    if value is None or value == "" or (isinstance(value, float) and math.isnan(value)):
        return None
    try:
        return float(value)
    except:
        return None

def safe_int(value):
    """Converts value to int, returns None if empty."""
    if value is None or value == "" or (isinstance(value, float) and math.isnan(value)):
        return None
    try:
        return int(float(value))
    except:
        return None



# --- 3. ADMIN AUTHENTICATION (VIP LIST RESTORED) ---
class AdminAuth(AuthenticationBackend):
    async def login(self, request: Request) -> bool:
        form = await request.form()
        username, password = form.get("username"), form.get("password")

        # --- VIP LIST START ---
        # 1. Define allowed users
        allowed_users = ["admin", "chickenman"]
        
        # 2. Check if username is in the list
        if username not in allowed_users:
            print(f"🚫 Access Denied: '{username}' is not in the VIP list.")
            return False
        # --- VIP LIST END ---

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
        user = request.session.get("user")
        
        # --- VIP LIST CHECK FOR SESSION ---
        allowed_users = ["admin", "chickenman"]
        
        if user in allowed_users:
            return True
            
        return False

authentication_backend = AdminAuth(secret_key="super_secret_static_key")

# --- 4. ADMIN PANEL SETUP ---
class UserAdmin(ModelView, model=User):
    column_list = [User.id, User.username]
    form_args = dict(password_hash=dict(label="Password (Leave blank to keep current)"))

    async def on_model_change(self, data, model, is_created, request):
        incoming_password = data.get("password_hash")
        
        # 1. Safety Checks
        if not incoming_password:
            if not is_created: del data["password_hash"]
            return

        # 2. Ignore if it is already a hash
        if len(incoming_password) == 60 and incoming_password.startswith("$"):
            del data["password_hash"]
            return

        # 3. Hash the password
        hashed = get_password_hash(incoming_password[:70])
        
        # 4. 👇 THIS IS THE MISSING FIX
        # We must update BOTH the model object AND the data dictionary
        model.password_hash = hashed
        data["password_hash"] = hashed  # <--- Forces sqladmin to save the hash, not the plain text!

admin = Admin(app, engine, authentication_backend=authentication_backend)
admin.add_view(UserAdmin)

# --- 5. ROUTES ---
@app.get("/")
async def read_root(request: Request, session: Session = Depends(get_session)):
    user = request.session.get("user")
    
    # 1. Check if user is logged in
    if not user:
        # If NO user, kick them to /login
        return RedirectResponse(url="/login", status_code=303)

    # 2. If YES user, show the Dashboard
    return templates.TemplateResponse("homepage.html", { 
        "request": request, 
        "user": user 
    })

@app.get("/login")
async def login_page(request: Request):
    # Optional: If they are already logged in, send them straight to Dashboard
    if request.session.get("user"):
        return RedirectResponse(url="/", status_code=303)
        
    return login_templates.TemplateResponse("login.html", {"request": request})
@app.post("/login")
async def login_user(request: Request, username: str = Form(...), password: str = Form(...), session: Session = Depends(get_session)):
    user = session.exec(select(User).where(User.username == username)).first()
    if not user or not verify_password(password, user.password_hash):
        return "Invalid credentials!"
    request.session["user"] = user.username
    return RedirectResponse(url="/", status_code=303)

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/")

# --- DATA CLEANER ROUTES ---


# 1. Page Server
@app.get("/cleaner")
async def cleaner_page(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    return cleaner_templates.TemplateResponse("Datacleaner.html", {"request": request, "user": user})

# 2. Logic Server (Processes & Saves File)
@app.post("/clean-data")
async def clean_data(file: UploadFile = File(...), session: Session = Depends(get_session)):
    contents = await file.read()
    
    # Process Logic
    billing_df, ops_df, base_filename = process_dataframe(contents)
    
    if billing_df is None:
        return Response(content="Error processing file", status_code=400)

    # SAVE TO DATABASE (Same as before)

    records = billing_df.to_dict(orient='records')
    db_entries = []

    for record in records:
        entry = BillingRecord(
            # --- TEXT FIELDS (Use safe_str) ---
            trip_date=safe_str(record.get('TRIP_DATE')),
            trip_id=safe_str(record.get('TRIP_ID')),
            flight_no=safe_str(record.get('FLIGHT_NO.')),
            employee_id=safe_str(record.get('EMPLOYEE_ID')),
            employee_name=safe_str(record.get('EMPLOYEE_NAME')),
            gender=safe_str(record.get('GENDER')),
            address=safe_str(record.get('ADDRESS')),
            passenger_mobile=safe_str(record.get('PASSENGER_MOBILE')),
            landmark=safe_str(record.get('LANDMARK')),
            vehicle_no=safe_str(record.get('VEHICLE_NO')),
            direction=safe_str(record.get('DIRECTION')),
            shift_time=safe_str(record.get('SHIFT_TIME')),
            pax_no=safe_str(record.get('PAX_NO')),
            marshall=safe_str(record.get('MARSHALL')),
            reporting_location=safe_str(record.get('REPORTING_LOCATION')),

            # --- NUMERIC FIELDS (Use safe_float or safe_int) ---
            # This fixes the "invalid input syntax" error
            emp_count=safe_float(record.get('EMP_COUNT')) 
        )
        db_entries.append(entry)

    session.add_all(db_entries)
    session.commit()

    # SAVE EXCEL FILES LOCALLY
    # Ensure a 'generated' folder exists
    generated_dir = BASE_DIR.parent / "client" / "DataCleaner" / "generated"
    os.makedirs(generated_dir, exist_ok=True)

    billing_filename = f"BILLING_{base_filename}.xlsx"
    ops_filename = f"OPS_{base_filename}.xlsx"
    
    # Save Billing
    billing_excel = to_excel_billing(billing_df)
    with open(generated_dir / billing_filename, "wb") as f:
        f.write(billing_excel.read())

    # Save Ops
    ops_excel = to_excel_operations(ops_df)
    with open(generated_dir / ops_filename, "wb") as f:
        f.write(ops_excel.read())

    # Return JSON with filenames (Client will use these to request downloads)
    return {
        "status": "success",
        "billing_file": billing_filename,
        "ops_file": ops_filename
    }

# 3. Secure Download Endpoint
@app.get("/download/{filename}")
async def download_file(filename: str, request: Request):
    user = request.session.get("user")
    if not user:
        return Response("Unauthorized", status_code=401)
    
    file_path = BASE_DIR.parent / "client" / "DataCleaner" / "generated" / filename
    if not file_path.exists():
        return Response("File not found", status_code=404)

    return FileResponse(path=file_path, filename=filename, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')




class TripDataAdmin(ModelView, model=TripData):
    column_list = [
        TripData.date, 
        TripData.trip_id, 
        TripData.employee_name, 
        TripData.arrival_time, 
        TripData.leave_time,
        TripData.departure_or_parking_time,
        TripData.gps_remarks,
        TripData.una,
        TripData.route_missing,
        TripData.clubbing_missing,
        TripData.one_side,
        TripData.two_side,
        TripData.club_km,
        TripData.passed,
        TripData.b2b_deducted,
        TripData.total_km_pass,
        TripData.billable_count,
        TripData.vendor,
        TripData.reporting_at,
        TripData.staff_count,
        TripData.mis_remarks,
        TripData.bb,
        TripData.dd,
        TripData.team_type,
        TripData.gender,
        TripData.address,
        TripData.locality,
        TripData.cab_registration_no,
        TripData.cab_last_digit,
        TripData.cab_type,
        TripData.trip_direction,
        TripData.shift_time,
        TripData.marshall,
        TripData.employee_id,
        TripData.flight_number,
        TripData.one_side,
        TripData.two_side,
        TripData.club_km,
        TripData.passed,
        TripData.b2b_deducted,
        TripData.total_km_pass,
        TripData.billable_count,
        TripData.vendor,
        TripData.reporting_at,
        TripData.staff_count,
        TripData.mis_remarks,
        TripData.bb,
        TripData.una,
        TripData.route_missing,
        TripData.clubbing_missing,
        TripData.arrival_time,
        TripData.leave_time,
        TripData.departure_or_parking_time,
        TripData.gps_remarks,
    ]

# 3. Add it to the admin app
admin.add_view(TripDataAdmin)

# --- 2. SERVE THE HTML PAGE ---
@app.get("/gps-corner")
async def gps_page(request: Request):
    user = request.session.get("user")
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    return gps_templates.TemplateResponse("gps_corner.html", {"request": request, "user": user})

# --- 3. API: FETCH DATA ---
@app.get("/api/gps-data")
async def get_gps_data(
    date: str = None, 
    direction: str = None, 
    cab: str = None, 
    clubbing: str = None,
    page: int = 1,       # 👈 New: Accepts Page Number
    limit: int = 50,     # 👈 New: Accepts Limit (Default 50)
    session: Session = Depends(get_session)
):
    query = select(TripData)
    
    # 1. DATE FILTER (Fixing the format mismatch)
    if date:
        # The HTML sends "YYYY-MM-DD", but your DB likely has "DD-MM-YYYY"
        try:
            parts = date.split("-")
            # If input is 2025-02-11, convert to 11-02-2025
            if len(parts[0]) == 4: 
                formatted_date = f"{parts[2]}-{parts[1]}-{parts[0]}"
                query = query.where(TripData.date == formatted_date)
            else:
                query = query.where(TripData.date == date)
        except:
            pass # Ignore errors if date is weird
            
    # 2. OTHER FILTERS
    if direction and direction != "All":
        query = query.where(TripData.trip_direction == direction)
    if cab:
        # Case insensitive search for Cab No
        query = query.where(TripData.vehicle_no.contains(cab))
    if clubbing and clubbing != "All":
        query = query.where(TripData.clubbing_missing == clubbing)

    # 3. SORTING & PAGINATION
    # Sort by ID descending (Newest first)
    query = query.order_by(desc(TripData.id))
    
    # Calculate Offset (Skip logic)
    offset = (page - 1) * limit
    query = query.offset(offset).limit(limit)
    
    results = session.exec(query).all()
    return results

# --- 4. API: UPDATE DATA ---
class GPSUpdateModel(BaseModel):
    id: int
    arrival_time: Optional[str] = None
    departure_parking_time: Optional[str] = None
    leave_time: Optional[str] = None
    gps_remarks: Optional[str] = None

@app.post("/api/gps-update")
async def update_gps_data(data: GPSUpdateModel, session: Session = Depends(get_session)):
    trip = session.get(TripData, data.id)
    if not trip:
        return Response("Trip not found", status_code=404)
    
    # Update Fields
    trip.arrival_time = data.arrival_time
    trip.departure_parking_time = data.departure_parking_time
    trip.leave_time = data.leave_time
    trip.gps_remarks = data.gps_remarks
    
    session.add(trip)
    session.commit()
    return {"status": "success"}