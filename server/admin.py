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

