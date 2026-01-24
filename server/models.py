from sqlmodel import SQLModel, Field
from typing import Optional

# --- 1. USER & AUTH ---
class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(index=True, unique=True)
    password_hash: str
    

# --- 2. GPS CORNER DATA (Existing) ---
class TripData(SQLModel, table=True):
    __tablename__ = "tripdata"
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Composite Key (Trip ID + Employee ID)
    unique_id: str = Field(index=True, unique=True)

    # --- MANDATORY COLUMNS (Matches FINAL_DB_MAP) ---
    shift_date: Optional[str] = None
    trip_id: Optional[str] = None
    employee_id: Optional[str] = None
    gender: Optional[str] = None
    emp_category: Optional[str] = None
    employee_name: Optional[str] = None
    shift_time: Optional[str] = None
    pickup_time: Optional[str] = None
    drop_time: Optional[str] = None
    trip_direction: Optional[str] = None
    cab_reg_no: Optional[str] = None
    cab_type: Optional[str] = None
    vendor: Optional[str] = None
    office: Optional[str] = None
    airport_name: Optional[str] = None
    landmark: Optional[str] = None
    address: Optional[str] = None
    flight_number: Optional[str] = None
    flight_category: Optional[str] = None
    flight_route: Optional[str] = None
    flight_type: Optional[str] = None
    trip_date: Optional[str] = None
    mis_remark: Optional[str] = None
    in_app_extra: Optional[str] = None
    
    # Special columns you requested
    una: Optional[str] = None
    unique_id: Optional[str] = None
    
    route_status: Optional[str] = None
    clubbing_status: Optional[str] = None
    journey_start_location: Optional[str] = None
    journey_end_location: Optional[str] = None
    gps_time: Optional[str] = None
    gps_remark: Optional[str] = None

    claim_status: Optional[str] = None
    marshall: Optional[str] = None
    staff_count: Optional[int] = None
    billable_count: Optional[int] = None
    one_side: Optional[float] = None
    two_side: Optional[float] = None
    club_km: Optional[float] = None
    passed: Optional[float] = None
    b2b_deducted: Optional[float] = None
    total_km_pass: Optional[float] = None
    dd: Optional[str] = None
    


# --- 3. CLEANER: CLIENT DATA ---
class ClientData(SQLModel, table=True):
      # Internal Database ID
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Composite Key (Trip ID + Employee ID)
    unique_id: str = Field(index=True, unique=True)

    # --- MANDATORY COLUMNS (Matches FINAL_DB_MAP) ---
    shift_date: Optional[str] = None
    trip_id: Optional[str] = None
    employee_id: Optional[str] = None
    gender: Optional[str] = None
    emp_category: Optional[str] = None
    employee_name: Optional[str] = None
    shift_time: Optional[str] = None
    pickup_time: Optional[str] = None
    drop_time: Optional[str] = None
    trip_direction: Optional[str] = None
    cab_reg_no: Optional[str] = None
    cab_type: Optional[str] = None
    vendor: Optional[str] = None
    office: Optional[str] = None
    airport_name: Optional[str] = None
    landmark: Optional[str] = None
    address: Optional[str] = None
    flight_number: Optional[str] = None
    flight_category: Optional[str] = None
    flight_route: Optional[str] = None
    flight_type: Optional[str] = None
    trip_date: Optional[str] = None
    mis_remark: Optional[str] = None
    in_app_extra: Optional[str] = None
    
    # Special columns you requested
    una: Optional[str] = None
    unique_id: Optional[str] = None
    
    route_status: Optional[str] = None
    clubbing_status: Optional[str] = None
    gps_time: Optional[str] = None
    gps_remark: Optional[str] = None


class RawTripData(SQLModel, table=True):
    # Internal Database ID
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Composite Key (Trip ID + Employee ID)
    unique_id: str = Field(index=True, unique=True)

    # --- MANDATORY COLUMNS (Matches FINAL_DB_MAP) ---
    shift_date: Optional[str] = None
    trip_id: Optional[str] = None
    employee_id: Optional[str] = None
    gender: Optional[str] = None
    emp_category: Optional[str] = None
    employee_name: Optional[str] = None
    shift_time: Optional[str] = None
    pickup_time: Optional[str] = None
    drop_time: Optional[str] = None
    trip_direction: Optional[str] = None
    cab_reg_no: Optional[str] = None
    cab_type: Optional[str] = None
    vendor: Optional[str] = None
    office: Optional[str] = None
    airport_name: Optional[str] = None
    landmark: Optional[str] = None
    address: Optional[str] = None
    flight_number: Optional[str] = None
    flight_category: Optional[str] = None
    flight_route: Optional[str] = None
    flight_type: Optional[str] = None
    trip_date: Optional[str] = None
    mis_remark: Optional[str] = None
    in_app_extra: Optional[str] = None
    
    # Special columns you requested
    una: Optional[str] = None
    unique_id: Optional[str] = None
    
    route_status: Optional[str] = None
    clubbing_status: Optional[str] = None
    gps_time: Optional[str] = None
    gps_remark: Optional[str] = None
    
    passenger_mobile: Optional[str] = None
    driver_name: Optional[str] = None
    driver_mobile: Optional[str] = None

# --- 5. CLEANER: OPERATION DATA ---
class OperationData(SQLModel, table=True):
      # Internal Database ID
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Composite Key (Trip ID + Employee ID)
    unique_id: str = Field(index=True, unique=True)

    # --- MANDATORY COLUMNS (Matches FINAL_DB_MAP) ---
    shift_date: Optional[str] = None
    trip_id: Optional[str] = None
    employee_id: Optional[str] = None
    gender: Optional[str] = None
    emp_category: Optional[str] = None
    employee_name: Optional[str] = None
    shift_time: Optional[str] = None
    pickup_time: Optional[str] = None
    drop_time: Optional[str] = None
    trip_direction: Optional[str] = None
    cab_reg_no: Optional[str] = None
    cab_type: Optional[str] = None
    vendor: Optional[str] = None
    office: Optional[str] = None
    airport_name: Optional[str] = None
    landmark: Optional[str] = None
    address: Optional[str] = None
    flight_number: Optional[str] = None
    flight_category: Optional[str] = None
    flight_route: Optional[str] = None
    flight_type: Optional[str] = None
    trip_date: Optional[str] = None
    mis_remark: Optional[str] = None
    in_app_extra: Optional[str] = None
    
    # Special columns you requested
    una: Optional[str] = None
    unique_id: Optional[str] = None
    
    route_status: Optional[str] = None
    clubbing_status: Optional[str] = None
    gps_time: Optional[str] = None
    gps_remark: Optional[str] = None



class BARowData(SQLModel, table=True):
    __tablename__ = "ba_row_data"
    
    # We use Trip Id as the unique identifier
    trip_id: int = Field(primary_key=True, alias="Trip Id") 
    
    leg_date: Optional[str] = Field(default=None, alias="Leg Date")
    employee_id: Optional[str] = Field(default=None, alias="Employee ID")
    employee_name: Optional[str] = Field(default=None, alias="Employee Name")
    gender: Optional[str] = Field(default=None, alias="Gender")
    emp_category: Optional[str] = Field(default=None, alias="EMP_CATEGORY")
    
    shift_time: Optional[str] = Field(default=None, alias="Shift Time")
    pickup_time: Optional[str] = Field(default=None, alias="Pickup Time")
    drop_time: Optional[str] = Field(default=None, alias="Drop Time")
    trip_direction: Optional[str] = Field(default=None, alias="Trip Direction")
    
    registration: Optional[str] = Field(default=None, alias="Registration")
    cab_type: Optional[str] = Field(default=None, alias="Cab Type")
    vendor: Optional[str] = Field(default=None, alias="Vendor")
    office: Optional[str] = Field(default=None, alias="Office")
    
    airport_name: Optional[str] = Field(default=None, alias="Airport Name")
    landmark: Optional[str] = Field(default=None, alias="Landmark")
    address: Optional[str] = Field(default=None, alias="Address")
    
    # Audit & Remarks
    ba_remark: Optional[str] = Field(default=None, alias="BA REMARK")
    mis_remark: Optional[str] = Field(default=None, alias="MiS Remark")
    route_status: Optional[str] = Field(default=None, alias="Route Status")
    trip_date: Optional[str] = Field(default=None, alias="Trip Date")
    
    # Allow extra columns to be stored flexibly if needed, or define strict fields
    class Config:
        arbitrary_types_allowed = True



# 1. Zone & KM Table (The Base)
class T3ZoneKm(SQLModel, table=True):
    __tablename__ = "t3_zone_km"
    
    # Zone must be the Primary Key to be referenced by others
    zone: str = Field(primary_key=True) 
    km: str  # e.g., "0-15"

# 2. Locality & Zone Table (Links to Zone)
class T3LocalityZone(SQLModel, table=True):
    __tablename__ = "t3_locality_zone"
    
    # Locality must be Primary Key to be referenced by Address
    locality: str = Field(primary_key=True)
    
    # Links to T3ZoneKm.zone
    zone: Optional[str] = Field(default=None, foreign_key="t3_zone_km.zone")

# 3. Address Table (Links to Locality)
class T3AddressLocality(SQLModel, table=True):
    __tablename__ = "t3_address_locality"
    
    # It is cleaner to use an integer ID as Primary Key
    id: Optional[int] = Field(default=None, primary_key=True)
    
    address: str = Field(index=True, unique=True)
    
    # Links to T3LocalityZone.locality
    locality: Optional[str] = Field(default=None, foreign_key="t3_locality_zone.locality")
    
    # These should NOT be Foreign Keys. They are derived/cached data.
    zone: Optional[str] = Field(default=None)
    km: Optional[str] = Field(default=None)


class vehicle_master(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    vehicle_id: Optional[str] = None
    vehicle_no: Optional[str] = None
    vehicle_type: Optional[str] = None
    vehicle_registration_no: Optional[str] = None
    vehicle_model: Optional[str] = None
    vehicle_owner: Optional[str] = None
    vehicle_owner_name: Optional[str] = None
    vehicle_owner_mobile: Optional[str] = None
    vehicle_driver_name: Optional[str] = None
    vehicle_driver_mobile: Optional[str] = None
    vehicle_rc: Optional[str] = None

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
