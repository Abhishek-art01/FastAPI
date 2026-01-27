from sqlmodel import SQLModel, Field
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, ConfigDict

# Base model with dynamic column support
class DynamicSQLModel(SQLModel):
    """Base model that allows dynamic columns"""
    model_config = ConfigDict(extra='allow')
    
    # Store any extra fields that don't match defined columns
    _extra_fields: Dict[str, Any] = {}

# --- 1. USER & AUTH ---
class User(DynamicSQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(index=True, unique=True)
    password_hash: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

# --- 2. GPS CORNER DATA (Existing) ---
class TripData(DynamicSQLModel, table=True):
    __tablename__ = "trip_data"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    unique_id: str = Field(index=True, unique=True)
    
    # --- MANDATORY COLUMNS ---
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
    
    # Special columns
    una: Optional[str] = None
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
    
    # Metadata
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    source_file: Optional[str] = None

# --- 3. CLEANER: CLIENT DATA ---
class ClientData(DynamicSQLModel, table=True):
    __tablename__ = "client_data"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    unique_id: str = Field(index=True, unique=True)
    
    # --- MANDATORY COLUMNS ---
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
    
    # Special columns
    una: Optional[str] = None
    route_status: Optional[str] = None
    clubbing_status: Optional[str] = None
    gps_time: Optional[str] = None
    gps_remark: Optional[str] = None
    
    # Metadata
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    client_name: Optional[str] = None
    data_source: Optional[str] = None

# --- 4. RAW TRIP DATA ---
class RawTripData(DynamicSQLModel, table=True):
    __tablename__ = "raw_trip_data"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    unique_id: str = Field(index=True, unique=True)
    
    # --- MANDATORY COLUMNS ---
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
    
    # Special columns
    una: Optional[str] = None
    route_status: Optional[str] = None
    clubbing_status: Optional[str] = None
    gps_time: Optional[str] = None
    gps_remark: Optional[str] = None
    
    # Additional raw data fields
    passenger_mobile: Optional[str] = None
    driver_name: Optional[str] = None
    driver_mobile: Optional[str] = None
    
    # Metadata
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    raw_source: Optional[str] = None
    file_name: Optional[str] = None

# --- 5. CLEANER: OPERATION DATA ---
class OperationData(DynamicSQLModel, table=True):
    __tablename__ = "operation_data"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    unique_id: str = Field(index=True, unique=True)
    
    # --- MANDATORY COLUMNS ---
    shift_date: Optional[str] = None
    trip_id: Optional[str] = None
    employee_id: Optional[str] = None
    employee_gender: Optional[str] = None
    employee_category: Optional[str] = None
    employee_name: Optional[str] = None
    shift_time: Optional[str] = None
    pickup_time: Optional[str] = None
    drop_time: Optional[str] = None
    trip_direction: Optional[str] = None
    cab_registration_no: Optional[str] = None
    cab_type: Optional[str] = None
    vendor: Optional[str] = None
    office: Optional[str] = None
    airport_name: Optional[str] = None
    landmark: Optional[str] = None
    employee_address: Optional[str] = None
    flight_number: Optional[str] = None
    flight_category: Optional[str] = None
    flight_route: Optional[str] = None
    flight_type: Optional[str] = None
    trip_date: Optional[str] = None
    mis_remark: Optional[str] = None
    data_source: Optional[str] = None
    unique_id: Optional[str] = None
    
    # Special columns
    una: Optional[str] = None
    ba_remark: Optional[str] = None
    toll_route: Optional[str] = None
    route_status: Optional[str] = None
    clubbing_status: Optional[str] = None
    gps_time: Optional[str] = None
    gps_remark: Optional[str] = None
    
    # Metadata
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    operation_type: Optional[str] = None
    processed_by: Optional[str] = None

# --- 6. BA ROW DATA ---
class BARowData(DynamicSQLModel, table=True):
    __tablename__ = "ba_row_data"
    
    trip_id: int = Field(primary_key=True, alias="trip_id") 
    
    leg_date: Optional[str] = Field(default=None, alias="leg_date")
    employee_id: Optional[str] = Field(default=None, alias="employee_id")
    employee_name: Optional[str] = Field(default=None, alias="employee_name")
    gender: Optional[str] = Field(default=None, alias="gender")
    emp_category: Optional[str] = Field(default=None, alias="emp_category")
    
    shift_time: Optional[str] = Field(default=None, alias="shift_time")
    pickup_time: Optional[str] = Field(default=None, alias="pickup_time")
    drop_time: Optional[str] = Field(default=None, alias="drop_time")
    trip_direction: Optional[str] = Field(default=None, alias="trip_direction")
    
    registration: Optional[str] = Field(default=None, alias="registration")
    cab_type: Optional[str] = Field(default=None, alias="cab_type")
    vendor: Optional[str] = Field(default=None, alias="vendor")
    office: Optional[str] = Field(default=None, alias="office")
    
    airport_name: Optional[str] = Field(default=None, alias="airport_name")
    landmark: Optional[str] = Field(default=None, alias="landmark")
    address: Optional[str] = Field(default=None, alias="address")
    
    # Audit & Remarks
    ba_remark: Optional[str] = Field(default=None, alias="ba_remark")
    mis_remark: Optional[str] = Field(default=None, alias="mis_remark")
    route_status: Optional[str] = Field(default=None, alias="route_status")
    trip_date: Optional[str] = Field(default=None, alias="trip_date")
    
    # Metadata
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    ba_agent: Optional[str] = None

# --- 7. ZONE & KM TABLES ---
class T3ZoneKm(DynamicSQLModel, table=True):
    __tablename__ = "t3_zone_km"
    
    zone: str = Field(primary_key=True)
    km: str
    
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    created_by: Optional[str] = None

class T3LocalityZone(DynamicSQLModel, table=True):
    __tablename__ = "t3_locality_zone"
    
    locality: str = Field(primary_key=True)
    zone: Optional[str] = Field(default=None, foreign_key="t3_zone_km.zone")
    
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    created_by: Optional[str] = None

class T3AddressLocality(DynamicSQLModel, table=True):
    __tablename__ = "t3_address_locality"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    address: str = Field(index=True, unique=True)
    locality: Optional[str] = Field(default=None, foreign_key="t3_locality_zone.locality")
    zone: Optional[str] = Field(default=None)
    km: Optional[str] = Field(default=None)
    
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    verified: Optional[bool] = False
    verified_by: Optional[str] = None

# --- 8. VEHICLE MASTER ---
class VehicleMaster(DynamicSQLModel, table=True):
    __tablename__ = "vehicle_master"
    
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
    
    # Additional fields
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    is_active: Optional[bool] = True
    insurance_valid_until: Optional[str] = None
    fitness_certificate_valid_until: Optional[str] = None

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

# --- Dynamic Column Management ---
class DynamicColumnSchema(BaseModel):
    model_name: str
    column_name: str
    column_type: str
    default_value: Optional[Any] = None
    is_nullable: bool = True

class TableSchemaResponse(BaseModel):
    table_name: str
    columns: List[Dict[str, Any]]
    row_count: Optional[int] = None