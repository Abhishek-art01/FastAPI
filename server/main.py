from fastapi import FastAPI, Depends, Request, Form, Response
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

# Import local modules
from .database import engine, get_session, create_db_and_tables
from .models import Hero, User
from .auth import verify_password, get_password_hash

# --- SETUP PATHS ---
BASE_DIR = Path(__file__).resolve().parent
CLIENT_DIR = BASE_DIR.parent / "client" / "HomePage"
LOGIN_DIR = BASE_DIR.parent / "client" / "LoginPage"

@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield

app = FastAPI(lifespan=lifespan)

# --- 1. MIDDLEWARE (The Fix for Render) ---
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
templates = Jinja2Templates(directory=CLIENT_DIR)
login_templates = Jinja2Templates(directory=LOGIN_DIR)

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

class HeroAdmin(ModelView, model=Hero):
    column_list = [Hero.id, Hero.name]

admin = Admin(app, engine, authentication_backend=authentication_backend)
admin.add_view(HeroAdmin)
admin.add_view(UserAdmin)

# --- 5. ROUTES ---
@app.get("/")
async def read_root(request: Request, session: Session = Depends(get_session)):
    heroes = session.exec(select(Hero)).all()
    user = request.session.get("user")
    return templates.TemplateResponse("homepage.html", { "request": request, "heroes": heroes, "user": user })

@app.get("/login")
async def login_page(request: Request):
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