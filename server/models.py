from sqlmodel import SQLModel, Field
from typing import Optional

# --- 1. USER & AUTH ---
class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(index=True, unique=True)
    password_hash: str

# --- 2. GPS CORNER DATA (Existing) ---
class TripData(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    date: Optional[str] = None
    trip_id: Optional[str] = None
    flight_number: Optional[str] = None
    employee_id: Optional[str] = None
    team_type: Optional[str] = None
    gender: Optional[str] = None
    employee_name: Optional[str] = None
    address: Optional[str] = None
    locality: Optional[str] = None
    cab_registration_no: Optional[str] = None
    cab_last_digit: Optional[str] = None
    cab_type: Optional[str] = None
    trip_direction: Optional[str] = None
    shift_time: Optional[str] = None
    marshall: Optional[str] = None
    one_side: Optional[float] = None
    two_side: Optional[float] = None
    club_km: Optional[float] = None
    passed: Optional[float] = None
    b2b_deducted: Optional[float] = None
    total_km_pass: Optional[float] = None
    dd: Optional[str] = None
    billable_count: Optional[int] = None
    vendor: Optional[str] = None
    reporting_at: Optional[str] = None
    staff_count: Optional[int] = None
    mis_remarks: Optional[str] = None
    bb: Optional[str] = None
    una: Optional[str] = None
    route_missing: Optional[str] = None
    clubbing_missing: Optional[str] = None
    arrival_time: Optional[str] = None
    leave_time: Optional[str] = None
    departure_or_parking_time: Optional[str] = None 
    gps_remarks: Optional[str] = None
    vehicle_no: Optional[str] = None # Added for compatibility

# --- 3. CLEANER: CLIENT DATA ---
class ClientData(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    unique_id: str = Field(index=True, unique=True) # Composite Key
    
    shift_date: Optional[str] = None
    trip_id: Optional[str] = None
    employee_id: Optional[str] = None
    gender: Optional[str] = None
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

# --- 4. CLEANER: RAW DATA ---
class RawTripData(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    unique_id: str = Field(index=True, unique=True) # Composite Key

    trip_date: Optional[str] = None
    trip_id: Optional[str] = None
    agency_name: Optional[str] = None
    flight_no: Optional[str] = None
    employee_id: Optional[str] = None
    employee_name: Optional[str] = None
    gender: Optional[str] = None
    emp_category: Optional[str] = None
    address: Optional[str] = None
    passenger_mobile: Optional[str] = None
    landmark: Optional[str] = None
    vehicle_no: Optional[str] = None
    driver_name: Optional[str] = None
    driver_mobile: Optional[str] = None
    distance: Optional[str] = None
    direction: Optional[str] = None
    shift_time: Optional[str] = None
    reporting_time: Optional[str] = None
    reporting_location: Optional[str] = None
    emp_count: Optional[str] = None
    pax_no: Optional[str] = None
    marshall: Optional[str] = None
    trip_count: Optional[str] = None

# --- 5. CLEANER: OPERATION DATA ---
class OperationData(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    unique_id: str = Field(index=True, unique=True) # Composite Key

    date: Optional[str] = None
    trip_id: Optional[str] = None
    flight_number: Optional[str] = None
    employee_id: Optional[str] = None
    employee_name: Optional[str] = None
    address: Optional[str] = None
    pickup_location: Optional[str] = None
    drop_location: Optional[str] = None
    cab_no: Optional[str] = None
    shift_time: Optional[str] = None
    guard_route: Optional[str] = None
    remarks: Optional[str] = None