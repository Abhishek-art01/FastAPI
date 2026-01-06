from typing import Optional
from sqlmodel import Field, SQLModel

class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(index=True, unique=True)
    password_hash: str

class BillingRecord(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    trip_date: str
    trip_id: str
    flight_no: Optional[str] = None
    employee_id: Optional[str] = None
    employee_name: Optional[str] = None
    gender: Optional[str] = None
    address: Optional[str] = None
    passenger_mobile: Optional[str] = None
    landmark: Optional[str] = None
    vehicle_no: Optional[str] = None
    direction: Optional[str] = None
    shift_time: Optional[str] = None
    emp_count: Optional[float] = None
    pax_no: Optional[str] = None
    marshall: Optional[str] = None
    reporting_location: Optional[str] = None


from sqlmodel import SQLModel, Field
from typing import Optional

# ... existing models (Hero, User, BillingRecord) ...

from sqlmodel import SQLModel, Field
from typing import Optional

class TripData(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # --- EXISTING COLUMNS ---
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
    
    # Numerical Fields (KM / Costs)
    one_side: Optional[float] = None
    two_side: Optional[float] = None
    club_km: Optional[float] = None
    passed: Optional[float] = None
    b2b_deducted: Optional[float] = None
    total_km_pass: Optional[float] = None
    
    # Codes & Counts
    dd: Optional[str] = None
    billable_count: Optional[int] = None
    vendor: Optional[str] = None
    reporting_at: Optional[str] = None
    staff_count: Optional[int] = None
    
    # Remarks & Extras
    mis_remarks: Optional[str] = None
    bb: Optional[str] = None
    una: Optional[str] = None
    
    # --- YOUR NEW COLUMNS (Fixed) ---
    route_missing: Optional[str] = None
    clubbing_missing: Optional[str] = None
    arrival_time: Optional[str] = None        # Fixed typo "arrivel" -> "arrival"
    leave_time: Optional[str] = None          # Fixed casing "LEAVE" -> "leave"
    
    # 👇 FIXED: Replaced '/' with '_'
    departure_or_parking_time: Optional[str] = None 
    
    gps_remarks: Optional[str] = None