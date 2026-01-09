import os
import io
from pathlib import Path
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import FastAPI, Depends, Request, Form, Response, UploadFile, File
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware

# SQLModel imports (Including 'col' for bulk filtering)
from sqlmodel import select, Session, desc, col 

from sqladmin import Admin, ModelView
from sqladmin.authentication import AuthenticationBackend
from pydantic import BaseModel

# --- INTERNAL IMPORTS ---
from .auth import verify_password, get_password_hash
from .database import create_db_and_tables, get_session, engine
from .models import User, ClientData, RawTripData, OperationData, TripData
from .cleaner import process_client_data, process_raw_data, process_operation_data

# --- 1. CONFIGURATION & PATHS ---
BASE_DIR = Path(__file__).resolve().parent
CLIENT_DIR = BASE_DIR.parent / "client"

# Dictionary to manage all paths cleanly
DIRS = {
    "home": CLIENT_DIR / "HomePage",
    "login": CLIENT_DIR / "LoginPage",
    "cleaner": CLIENT_DIR / "DataCleaner",
    "gps": CLIENT_DIR / "GPSCorner"
}

# --- 2. LIFESPAN (Startup) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield

app = FastAPI(lifespan=lifespan)

# --- 3. MIDDLEWARE ---
# 1. CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 2. Session (Secure on Render, Lax on Localhost)
on_render = os.environ.get("RENDER") is not None
app.add_middleware(
    SessionMiddleware,
    secret_key="super_secret_static_key",
    max_age=3600, 
    https_only=on_render, 
    same_site="lax"
)

# --- 4. STATIC FILES & TEMPLATES ---
# Mount static folders for CSS/JS
app.mount("/static", StaticFiles(directory=DIRS["home"]), name="static")
app.mount("/login-static", StaticFiles(directory=DIRS["login"]), name="login_static")
app.mount("/cleaner-static", StaticFiles(directory=DIRS["cleaner"]), name="cleaner_static")
app.mount("/gps-static", StaticFiles(directory=DIRS["gps"]), name="gps_static")

# Setup Templates
templates = {
    "home": Jinja2Templates(directory=DIRS["home"]),
    "login": Jinja2Templates(directory=DIRS["login"]),
    "cleaner": Jinja2Templates(directory=DIRS["cleaner"]),
    "gps": Jinja2Templates(directory=DIRS["gps"]),
}

# --- 5. AUTHENTICATION BACKEND ---
class AdminAuth(AuthenticationBackend):
    async def login(self, request: Request) -> bool:
        form = await request.form()
        username, password = form.get("username"), form.get("password")
        
        # VIP Access Control
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

class TripDataAdmin(ModelView, model=TripData):
    # Updated to match your models.py columns exactly
    column_list = [TripData.date, TripData.trip_id, TripData.employee_name, TripData.cab_registration_no, TripData.trip_direction]

# Simple views for the cleaned data tables
class ClientDataAdmin(ModelView, model=ClientData): column_list = [ClientData.id, ClientData.trip_id, ClientData.employee_name]
class RawTripDataAdmin(ModelView, model=RawTripData): column_list = [RawTripData.id, RawTripData.trip_id, RawTripData.trip_date]
class OperationDataAdmin(ModelView, model=OperationData): column_list = [OperationData.id, OperationData.trip_id]

# Initialize Admin
admin = Admin(app, engine, authentication_backend=AdminAuth(secret_key="super_secret_static_key"))
admin.add_view(UserAdmin)
admin.add_view(TripDataAdmin)
admin.add_view(ClientDataAdmin)
admin.add_view(RawTripDataAdmin)
admin.add_view(OperationDataAdmin)

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

# ==========================================
# 🚀 DATA CLEANER API (OPTIMIZED)
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

        # --- A. CLIENT DATA ---
        if cleanerType == "client":
            content = await files[0].read()
            df_result, excel_output, filename = process_client_data(content)
            
            # BULK SAVE (Fast & Safe)
            if df_result is not None and not df_result.empty and "unique_id" in df_result.columns:
                # 1. Get IDs from File
                incoming_ids = df_result["unique_id"].dropna().unique().tolist()
                
                # 2. Get IDs already in DB
                existing_ids = set(session.exec(select(ClientData.unique_id).where(col(ClientData.unique_id).in_(incoming_ids))).all())
                
                # 3. Filter New Rows
                new_rows = df_result[~df_result["unique_id"].isin(existing_ids)]
                
                # 4. Bulk Insert
                if not new_rows.empty:
                    records = [ClientData(**row.to_dict()) for _, row in new_rows.iterrows()]
                    session.add_all(records)
                    session.commit()
                    print(f"✅ Client Data: Added {len(new_rows)} rows.")
                else:
                    print("⚠️ Client Data: No new unique rows found.")

        # --- B. RAW DATA ---
        elif cleanerType == "raw":
            file_data = []
            for f in files:
                content = await f.read()
                file_data.append((f.filename, content))

            df_result, excel_output, filename = process_raw_data(file_data)
            
            # BULK SAVE
            if df_result is not None and not df_result.empty and "unique_id" in df_result.columns:
                incoming_ids = df_result["unique_id"].dropna().unique().tolist()
                existing_ids = set(session.exec(select(RawTripData.unique_id).where(col(RawTripData.unique_id).in_(incoming_ids))).all())
                new_rows = df_result[~df_result["unique_id"].isin(existing_ids)]
                
                if not new_rows.empty:
                    records = [RawTripData(**row.to_dict()) for _, row in new_rows.iterrows()]
                    session.add_all(records)
                    session.commit()
                    print(f"✅ Raw Data: Added {len(new_rows)} rows.")
                else:
                    print("⚠️ Raw Data: No new unique rows found.")

        # --- C. OPERATION DATA ---
        elif cleanerType == "operation":
            file_data = []
            for f in files:
                content = await f.read()
                file_data.append((f.filename, content))
                
            # Note: process_operation_data returns None for df_result to skip DB save
            df_result, excel_output, filename = process_operation_data(file_data)
            
            # 🛑 NO DATABASE SAVING FOR OPERATION DATA

        # --- FINALIZE ---
        if excel_output is None:
            return Response("Error processing data", status_code=400)

        # Save locally for download
        generated_dir = DIRS["cleaner"] / "generated"
        os.makedirs(generated_dir, exist_ok=True)
        
        save_path = generated_dir / filename
        with open(save_path, "wb") as f:
            f.write(excel_output.read())

        row_count = len(df_result) if df_result is not None else "Formatting Only"
        
        return {
            "status": "success",
            "file_url": filename,
            "rows_processed": row_count
        }

    except Exception as e:
        print(f"❌ Server Error: {e}")
        return Response(f"Internal Error: {e}", status_code=500)

# --- DOWNLOAD ROUTE ---
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

# ==========================================
# 9. GPS CORNER API
# ==========================================
class GPSUpdateModel(BaseModel):
    id: int
    arrival_time: Optional[str] = None
    departure_parking_time: Optional[str] = None
    leave_time: Optional[str] = None
    gps_remarks: Optional[str] = None

@app.get("/api/gps-data")
async def get_gps_data(
    date: str = None, 
    direction: str = None, 
    cab: str = None, 
    clubbing: str = None, 
    page: int = 1, 
    limit: int = 50, 
    session: Session = Depends(get_session)
):
    query = select(TripData)
    
    # 1. Date Filter
    if date:
        try:
            parts = date.split("-")
            formatted_date = f"{parts[2]}-{parts[1]}-{parts[0]}" if len(parts[0]) == 4 else date
            query = query.where(TripData.date == formatted_date)
        except:
            pass 

    # 2. Other Filters
    if direction and direction != "All":
        query = query.where(TripData.trip_direction == direction)
    if cab:
        query = query.where(TripData.vehicle_no.contains(cab))
    if clubbing and clubbing != "All":
        query = query.where(TripData.clubbing_missing == clubbing)

    # 3. Sorting & Pagination
    query = query.order_by(desc(TripData.id))
    query = query.offset((page - 1) * limit).limit(limit)
    
    return session.exec(query).all()

@app.post("/api/gps-update")
async def update_gps_data(data: GPSUpdateModel, session: Session = Depends(get_session)):
    trip = session.get(TripData, data.id)
    if not trip:
        return Response("Trip not found", status_code=404)
    
    trip.arrival_time = data.arrival_time
    trip.departure_parking_time = data.departure_parking_time
    trip.leave_time = data.leave_time
    trip.gps_remarks = data.gps_remarks
    
    session.add(trip)
    session.commit()
    return {"status": "success"}