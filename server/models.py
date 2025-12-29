from typing import Optional
from sqlmodel import Field, SQLModel

class Hero(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    secret_name: str
    age: Optional[int] = None

class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    username: str = Field(index=True, unique=True)
    password_hash: str  # 👈 This line is CRITICAL

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
