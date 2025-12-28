from fastapi import FastAPI, Depends, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles  
from fastapi.responses import RedirectResponse 
from sqlmodel import select, Session
from sqladmin import Admin, ModelView
from pathlib import Path
from contextlib import asynccontextmanager
from starlette.middleware.sessions import SessionMiddleware
from sqladmin.authentication import AuthenticationBackend
from fastapi import Response
# IMPORT FROM FILES
from .database import engine, get_session, create_db_and_tables
from .models import Hero, User
from .auth import verify_password, get_password_hash

# --- 1. SETUP PATHS ---
BASE_DIR = Path(__file__).resolve().parent
# Points to: Folder/client/HomePage
CLIENT_DIR = BASE_DIR.parent / "client" / "HomePage"
# Points to: Folder/client/LoginPage
LOGIN_DIR = BASE_DIR.parent / "client" / "LoginPage"


# --- 2. LIFESPAN (Startup Logic) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    SessionMiddleware, 
    secret_key="static_super_secret_key", 
    max_age=3600,
    https_only=True,   # ✅ Set to True for Render (HTTPS)
    same_site="lax"
)

# --- 3. MOUNT STATIC FILES (CRITICAL FOR CSS) --- 
# This tells FastAPI: "If browser asks for /static/x, look in CLIENT_DIR for x"
app.mount("/static", StaticFiles(directory=CLIENT_DIR), name="static")
app.mount("/login-static", StaticFiles(directory=LOGIN_DIR), name="login_static")

templates = Jinja2Templates(directory=CLIENT_DIR)
login_templates = Jinja2Templates(directory=LOGIN_DIR)

# --- 5. ADMIN SETUP ---
class HeroAdmin(ModelView, model=Hero):
    column_list = [Hero.id, Hero.name, Hero.secret_name]


class UserAdmin(ModelView, model=User):
    column_list = [User.id, User.username]
    
    form_args = {
        "password_hash": {
            "label": "Password (Leave blank to keep current)",
            "render_kw": {"value": "", "autocomplete": "new-password"}
        }
    }

    async def on_model_change(self, data, model, is_created, request):
        # 1. Get the password typed in the form
        incoming_password = data.get("password_hash")
        
        print(f"DEBUG: Incoming password is: {incoming_password}")  # 👈 Check your terminal for this!

        # 2. Safety: If empty...
        if not incoming_password:
            if not is_created:
                # If editing, remove key so we don't overwrite with empty string
                del data["password_hash"]
            print("DEBUG: Password empty, skipping.")
            return

        # 3. Safety: If it looks like an old hash (starts with $), ignore it
        if len(incoming_password) == 60 and incoming_password.startswith("$"):
            print("DEBUG: Detected existing hash. Ignoring.")
            del data["password_hash"]
            return

        # 4. Hashing Logic
        print("DEBUG: Hashing new password...")
        hashed = get_password_hash(incoming_password)
        
        # 5. CRITICAL FIX: Update BOTH model and data
        model.password_hash = hashed
        data["password_hash"] = hashed  # 👈 This forces sqladmin to use the hash
        
        print(f"DEBUG: Password successfully hashed to: {hashed[:10]}...")

class AdminAuth(AuthenticationBackend):
    async def login(self, request: Request) -> bool:
        form = await request.form()
        username, password = form.get("username"), form.get("password")

        # 👇 CHECK 1: Reject immediately if name is not "admin"
        # CHANGE THE BLOCK TO THIS
        allowed_users = ["admin", "chickenman"]

        if username not in allowed_users:
            print(f"🚫 Access Denied: '{username}' is not allowed.")
            return False

        print(f"🔐 ADMIN LOGIN ATTEMPT: {username}")

        # Check DB
        with Session(engine) as session:
            user = session.exec(select(User).where(User.username == username)).first()
            
            if not user:
                print("❌ User not found in DB")
                return False
            
            if verify_password(password, user.password_hash):
                print("✅ Password verified! Logging in...")
                request.session.update({"user": user.username}) 
                return True
            else:
                print("❌ Password incorrect")
        
        return False

    async def logout(self, request: Request) -> bool:
        request.session.clear()
        return True

    async def authenticate(self, request: Request) -> bool:
        user = request.session.get("user")
        
        # 👇 DEFINE YOUR VIP LIST HERE TOO
        allowed_users = ["admin", "chickenman"]
        
        # Check if the user is in the list
        if user in allowed_users:
            return True
            
        return False
# Initialize the auth backend
authentication_backend = AdminAuth(secret_key="same_secret_key_as_middleware")

admin = Admin(app, engine, authentication_backend=authentication_backend)
admin.add_view(HeroAdmin)
admin.add_view(UserAdmin)

# --- 6. ROUTES ---

# --- ROUTES ---

@app.get("/")
async def read_root(request: Request, session: Session = Depends(get_session)):
    heroes = session.exec(select(Hero)).all()
    return templates.TemplateResponse("homepage.html", {"request": request, "heroes": heroes})

@app.post("/heroes/")
async def create_hero(
    name: str = Form(...),
    secret_name: str = Form(...),
    age: int = Form(None),
    session: Session = Depends(get_session)
):
    hero = Hero(name=name, secret_name=secret_name, age=age)
    session.add(hero)
    session.commit()
    return RedirectResponse(url="/", status_code=303)

# --- LOGIN ROUTES ---

@app.get("/login")
async def login_page(request: Request):
    return login_templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login_user(
    username: str = Form(...), 
    password: str = Form(...), 
    session: Session = Depends(get_session)
):
    # Check DB for user
    user = session.exec(select(User).where(User.username == username)).first()
    
    if not user or not verify_password(password, user.password_hash):
        return "Invalid credentials!"
        
    return RedirectResponse(url="/", status_code=303)



@app.get("/debug-cookie")
def debug_cookie():
    response = Response(content="Check your cookies now!")
    response.set_cookie(
        key="test_cookie", 
        value="it_worked", 
        httponly=True, 
        samesite="strict",  # 👈 Match the middleware
        secure=False
    )
    return response