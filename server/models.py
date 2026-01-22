from sqlmodel import SQLModel, Field
from typing import Optional

# --- 1. USER & AUTH ---
class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(index=True, unique=True)
    password_hash: str

class TripDataDec2025(SQLModel, table=True):
    __tablename__ = "tripdata_dec2025"
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

class BARawTripData(SQLModel, table=True):
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



class T3AddressLocality(SQLModel, table=True):
    __tablename__ = "t3_address_locality"

    id: Optional[int] = Field(default=None)
    address: str = Field(index=True, unique=True,primary_key=True) 
    locality: Optional[str] = Field(default=None)
    zone : Optional[str] = Field(default=None, foreign_key="t3_locality_zone.zone")
    km : Optional[str] = Field(default=None, foreign_key="t3_zone_km.km")

class T3LocalityZone(SQLModel, table=True):
    __tablename__ = "t3_locality_zone"
    id: Optional[int] = Field(default=None)
    locality: Optional[str] = Field(index=True, unique=True,primary_key=True)
    zone: Optional[str] = None

class T3ZoneKm(SQLModel, table=True):
    __tablename__ = "t3_zone_km"
    id: Optional[int] = Field(default=None)
    zone: Optional[str] = Field(index=True, unique=True,primary_key=True)
    km: Optional[str] = None



class AITA75_35_address_locality(SQLModel, table=True):
    __tablename__ = "AITA75_35_address_locality"
    id: Optional[int] = Field(default=None, primary_key=True)
    address: Optional[str] = None
    locality: Optional[str] = None  

class AITA75_35_locality_zone(SQLModel, table=True):
    __tablename__ = "AITA75_35_locality_zone"
    id: Optional[int] = Field(default=None, primary_key=True)
    locality: Optional[str] = None
    zone: Optional[str] = None

class AITA75_35_zone_km(SQLModel, table=True):
    __tablename__ = "AITA75_35_zone_km"
    id: Optional[int] = Field(default=None, primary_key=True)
    zone: Optional[str] = None
    km: Optional[str] = None    

class AITA_VATIKA_address_locality(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    address: Optional[str] = None
    locality: Optional[str] = None

class AITA_VATIKA_locality_zone(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    locality: Optional[str] = None
    zone: Optional[str] = None

class AITA_VATIKA_zone_km(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    zone: Optional[str] = None
    km: Optional[str] = None    

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
