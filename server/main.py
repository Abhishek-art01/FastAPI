import os
import io
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
from .models import User, ClientData, RawTripData, OperationData, TripData 
from .cleaner import process_client_data, process_raw_data, process_operation_data

# --- 1. CONFIGURATION & PATHS ---
BASE_DIR = Path(__file__).resolve().parent
CLIENT_DIR = BASE_DIR.parent / "client"

DIRS = {
    "home": CLIENT_DIR / "HomePage",
    "login": CLIENT_DIR / "LoginPage",
    "cleaner": CLIENT_DIR / "DataCleaner",
    "gps": CLIENT_DIR / "GPSCorner",
    "operation-manager": CLIENT_DIR / "OperationManager",
    "sidebar": CLIENT_DIR / "Sidebar"
}

# --- 2. LIFESPAN (Startup) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
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
app.mount("/operation-manager-static", StaticFiles(directory=DIRS["operation-manager"]), name="operation-manager_static")
app.mount("/sidebar-static", StaticFiles(directory=DIRS["sidebar"]), name="sidebar_static")

templates = {
    "home": Jinja2Templates(directory=DIRS["home"]),
    "login": Jinja2Templates(directory=DIRS["login"]),
    "cleaner": Jinja2Templates(directory=DIRS["cleaner"]),
    "gps": Jinja2Templates(directory=DIRS["gps"]),
    "operation-manager": Jinja2Templates(directory=DIRS["operation-manager"]),
    "sidebar": Jinja2Templates(directory=DIRS["sidebar"]),
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

@app.get("/operation-manager")
async def operation_manager_page(request: Request):
    if not request.session.get("user"): return RedirectResponse(url="/login", status_code=303)
    return templates["operation-manager"].TemplateResponse("operation_manager.html", {"request": request})

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

        if cleanerType == "client":
            content = await files[0].read()
            df_result, excel_output, filename = process_client_data(content)
            
            if df_result is not None and not df_result.empty and "unique_id" in df_result.columns:
                incoming_ids = df_result["unique_id"].dropna().unique().tolist()
                existing_ids = set(session.exec(select(ClientData.unique_id).where(col(ClientData.unique_id).in_(incoming_ids))).all())
                new_rows = df_result[~df_result["unique_id"].isin(existing_ids)]
                if not new_rows.empty:
                    records = [ClientData(**row.to_dict()) for _, row in new_rows.iterrows()]
                    session.add_all(records)
                    session.commit()

        elif cleanerType == "raw":
            file_data = []
            for f in files:
                content = await f.read()
                file_data.append((f.filename, content))

            df_result, excel_output, filename = process_raw_data(file_data)
            
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
            "rows_processed": row_count
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