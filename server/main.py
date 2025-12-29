from fastapi import FastAPI, Depends, Request, Form, Response, UploadFile, File
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
from sqlmodel import select, Session
from sqladmin import Admin, ModelView
from sqladmin.authentication import AuthenticationBackend
from pathlib import Path
from contextlib import asynccontextmanager
import os
from fastapi.middleware.cors import CORSMiddleware
import io
import zipfile

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

templates = Jinja2Templates(directory=CLIENT_DIR)
login_templates = Jinja2Templates(directory=LOGIN_DIR)
cleaner_templates = Jinja2Templates(directory=CLEANER_DIR)


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

@app.get("/cleaner")
async def cleaner_page(request: Request):
    user = request.session.get("user")
    
    # Protect the route: Login Required
    if not user:
        return RedirectResponse(url="/login", status_code=303)
        
    return cleaner_templates.TemplateResponse("DataCleaner.html", {"request": request, "user": user})

@app.post("/clean-data")
async def clean_data(file: UploadFile = File(...), session: Session = Depends(get_session)):
    contents = await file.read()
    
    # 1. Process Logic (Now running from cleaner.py)
    billing_df, ops_df, filename = process_dataframe(contents)
    
    if billing_df is None:
        return Response(content="Error processing file", status_code=400)

    # 2. SAVE TO DATABASE
    records = billing_df.to_dict(orient='records')
    db_entries = []
    for record in records:
        entry = BillingRecord(
            trip_date=str(record.get('TRIP_DATE')),
            trip_id=str(record.get('TRIP_ID')),
            flight_no=str(record.get('FLIGHT_NO.')),
            employee_id=str(record.get('EMPLOYEE_ID')),
            employee_name=str(record.get('EMPLOYEE_NAME')),
            gender=str(record.get('GENDER')),
            address=str(record.get('ADDRESS')),
            passenger_mobile=str(record.get('PASSENGER_MOBILE')),
            landmark=str(record.get('LANDMARK')),
            vehicle_no=str(record.get('VEHICLE_NO')),
            direction=str(record.get('DIRECTION')),
            shift_time=str(record.get('SHIFT_TIME')),
            emp_count=record.get('EMP_COUNT'),
            pax_no=str(record.get('PAX_NO')),
            marshall=str(record.get('MARSHALL')),
            reporting_location=str(record.get('REPORTING_LOCATION'))
        )
        db_entries.append(entry)
    
    session.add_all(db_entries)
    session.commit()

    # 3. Create Excel Files (Using imported functions)
    billing_excel = to_excel_billing(billing_df)
    ops_excel = to_excel_operations(ops_df)

    # 4. ZIP Files
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr(f"BILLING_{filename}.xlsx", billing_excel.read())
        zip_file.writestr(f"OPS_{filename}.xlsx", ops_excel.read())

    # 5. Return Response
    zip_buffer.seek(0)
    return Response(
        content=zip_buffer.read(),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename=Cleaned_Data_{filename}.zip"}
    )