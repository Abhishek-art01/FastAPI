import pandas as pd
import numpy as np
import io
import xlrd
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side

# ==========================================
# 0. CONFIGURATION & GLOBAL HELPERS
# ==========================================

# 1. The Headers for Excel Output (Friendly Names)
MANDATORY_HEADERS = [
    'Shift Date', 'Trip ID', 'Employee ID', 'Gender', 'EMP_CATEGORY', 'Employee Name', 
    'Shift Time', 'Pickup Time', 'Drop Time', 'Trip Direction', 'Cab Reg No', 
    'Cab Type', 'Vendor', 'Office', 'Airport Name', 'Landmark', 'Address', 
    'Flight Number', 'Flight Category', 'Flight Route', 'Flight Type', 
    'Trip Date', 'MiS Remark', 'In App/ Extra', 'UNA2', 'UNA', 
    'Route Status', 'Clubbing Status', 'GPS TIME', 'GPS REMARK',
    'passenger_mobile', 'driver_name', 'DRIVER_MOBILE' 
]

# 2. Map Friendly Headers -> SQLModel Fields (snake_case)
MANDATORY_DB_MAP = {
    'Shift Date': 'shift_date', 'Trip ID': 'trip_id', 'Employee ID': 'employee_id', 
    'Gender': 'gender', 'EMP_CATEGORY': 'emp_category', 'Employee Name': 'employee_name', 
    'Shift Time': 'shift_time', 'Pickup Time': 'pickup_time', 'Drop Time': 'drop_time', 
    'Trip Direction': 'trip_direction', 'Cab Reg No': 'cab_reg_no', 'Cab Type': 'cab_type', 
    'Vendor': 'vendor', 'Office': 'office', 'Airport Name': 'airport_name', 
    'Landmark': 'landmark', 'Address': 'address', 'Flight Number': 'flight_number', 
    'Flight Category': 'flight_category', 'Flight Route': 'flight_route', 
    'Flight Type': 'flight_type', 'Trip Date': 'trip_date', 'MiS Remark': 'mis_remark', 
    'In App/ Extra': 'in_app_extra', 'UNA2': 'una2', 'UNA': 'una', 
    'Route Status': 'route_status', 'Clubbing Status': 'clubbing_status', 
    'GPS TIME': 'gps_time', 'GPS REMARK': 'gps_remark',
    'passenger_mobile': 'passenger_mobile', 'driver_name': 'driver_name', 'DRIVER_MOBILE': 'driver_mobile'
}

def standardize_dataframe(df):
    """
    Standardizes DataFrame to match SQLModel definitions:
    1. Generates 'unique_id' (Composite Key).
    2. Ensures ALL mapped columns exist (fills missing with "").
    3. Returns DataFrame with [Mandatory Columns] + [Any Extra Columns].
    """
    if df is None or df.empty:
        return None

    # 1. Generate Unique ID
    if 'trip_id' in df.columns and 'employee_id' in df.columns:
        df['unique_id'] = df['trip_id'].astype(str).str.strip() + df['employee_id'].astype(str).str.strip()
        # Filter invalid IDs
        df = df[
            df["unique_id"].notna() & 
            (df["unique_id"] != "") & 
            (~df["unique_id"].str.contains("nan|None", case=False))
        ]
    else:
        return None # Critical missing data

    # 2. Sync Columns with SQLModel (Fill Missing)
    target_cols = list(MANDATORY_DB_MAP.values())
    for col in target_cols:
        if col not in df.columns:
            df[col] = ""

    # 3. Populate specific logic columns
    df['una2'] = df['unique_id'] # As requested
    # Optional: populate 'una' if needed, or leave blank

    # 4. Organize Columns: Mandatory -> Extra -> unique_id
    current_cols = df.columns.tolist()
    reserved_cols = set(target_cols) | {'unique_id'}
    extra_cols = [c for c in current_cols if c not in reserved_cols]
    
    final_order = target_cols + extra_cols + ['unique_id']
    return df[final_order]

def get_xls_style_data(book, xf_index):
    """ Helper to extract style (Hex Color, Bold) from xlrd book. """
    try:
        xf = book.xf_list[xf_index]
        font = book.font_list[xf.font_index]
        
        def get_hex(idx):
            if idx is not None and idx < 64:
                rgb = book.colour_map.get(idx)
                if rgb and rgb != (0,0,0) and rgb != (255, 255, 255):
                    return "{:02x}{:02x}{:02x}".format(*rgb).upper()
            return None

        bg_hex = get_hex(xf.background.pattern_colour_index)
        font_hex = get_hex(font.colour_index)
        return bg_hex, font_hex, bool(font.bold)
    except:
        return None, None, False

def create_styled_excel(df, filename_prefix="Cleaned"):
    """ Generates Excel with consistent formatting. """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # Invert map to get Friendly Headers
        inv_map = {v: k for k, v in MANDATORY_DB_MAP.items()}
        df_export = df.rename(columns=inv_map)
        
        # Order: Mandatory Headers -> Extra Headers
        export_cols = [h for h in MANDATORY_HEADERS if h in df_export.columns]
        remaining = [c for c in df_export.columns if c not in MANDATORY_HEADERS and c != 'unique_id']
        export_cols += remaining
        
        df_export = df_export[export_cols]
        
        df_export.to_excel(writer, index=False, sheet_name='Raw_Data')
        
        # Apply Styles
        workbook = writer.book
        worksheet = writer.sheets['Raw_Data']
        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#0070C0', 'font_color': 'white'})
        
        for i, col in enumerate(df_export.columns):
            worksheet.write(0, i, col, header_fmt)
            worksheet.set_column(i, i, 15)
            
    output.seek(0)
    return df, output, f"{filename_prefix}.xlsx"


# ==========================================
# 1. CLIENT DATA CLEANER
# ==========================================
def process_client_data(file_content):
    try:
        # Columns to Remove based on your request
        DROP_COLS = [
            'Bunit ID', 'Cycle Start', 'Cycle End', 'Billing Period', 'Project', 'Cost Center', 
            'Department', 'Planned Emp Count', 'Travelled Emp Count', 'Billable Emp Count', 
            'No Show', 'Planned Escort', 'Actual Escort', 'Emp Km', 'Trip Cost', 
            'Trip AC Cost', 'Per Emp Cost', 'Escort Cost', 'Penalty', 'Vendor Penalty', 
            'Total Cost', 'Assigned Contract', 'Cab Contract', 'Billing Zone', 
            'Trip Billing Zone', 'Emp Sigin Type', 'Escort ID', 'Toll Cost', 
            'State Tax Cost', 'Parking Or Toll Cost', 'Per Employee Overhead Cost', 
            'Trip Source', 'Extra Kms Based On Billable Employee Count', 'Billing Kms', 
            'Actual Kms At Employee Level', 'Grid Km', 'Employee Adjustment Distance', 
            'Trip Adjustment', 'Total Distance'
        ]

        # Input Mapping
        COL_MAP = {
            "Shift Date": "shift_date", "Trip ID": "trip_id", "Employee ID": "employee_id", 
            "Gender": "gender", "Employee Name": "employee_name", "Shift Time": "shift_time", 
            "Pickup Time": "pickup_time", "Drop Time": "drop_time", "Trip Direction": "trip_direction", 
            "Cab Reg No": "cab_reg_no", "Cab Type": "cab_type", "Vendor": "vendor", 
            "Office": "office", "Airport Name": "airport_name", "Landmark": "landmark", 
            "Address": "address", "Flight Number": "flight_number", "Flight Category": "flight_category", 
            "Flight Route": "flight_route", "Flight Type": "flight_type"
        }
        
        df = pd.read_excel(io.BytesIO(file_content), dtype=str)
        
        # 1. Drop Unwanted
        df = df.drop(columns=[c for c in DROP_COLS if c in df.columns], errors='ignore')

        # 2. Data Cleaning
        if "Cab Reg No" in df.columns:
            df["Cab Reg No"] = df["Cab Reg No"].str.replace("-", "", regex=False).str.upper()

        if "Trip Direction" in df.columns:
            df["Trip Direction"] = df["Trip Direction"].astype(str).str.strip().str.title().replace({
                "Login": "Pickup", "Logout": "Drop"
            })

        # 3. Rename to DB Columns
        df_db = df.rename(columns=COL_MAP).fillna("")
        
        # 4. Standardize (Matches ClientData Model)
        df_db = standardize_dataframe(df_db)
        if df_db is None: return None, None, None

        return create_styled_excel(df_db, "Client_Cleaned")
    except Exception as e:
        print(f"❌ Client Cleaner Error: {e}")
        return None, None, None


# ==========================================
# 2. RAW DATA CLEANER
# ==========================================
def _clean_single_raw_df(df):
    try:
        # Trip ID Logic
        df["Trip_ID"] = np.where(df.iloc[:, 10].astype(str).str.startswith("T"), df.iloc[:, 10], np.nan)
        df["Trip_ID"] = df["Trip_ID"].ffill()

        # Identify Row Types
        is_header = df.iloc[:, 1].astype(str).str.contains("UNITED FACILITIES", na=False)
        is_passenger = df.iloc[:, 0].astype(str).str.match(r"^[1-5]$")

        # Extraction Maps
        h_map = {0: 'TRIP_DATE', 1: 'AGENCY_NAME', 2: 'D_LOGIN', 3: 'VEHICLE_NO', 4: 'DRIVER_NAME', 6: 'DRIVER_MOBILE', 7: 'MARSHALL', 8: 'DISTANCE', 9: 'EMP_COUNT', 10: 'TRIP_COUNT'}
        p_map = {0: 'PAX_NO', 1: 'REPORTING_TIME', 2: 'EMPLOYEE_ID', 3: 'EMPLOYEE_NAME', 4: 'GENDER', 5: 'EMP_CATEGORY', 6: 'FLIGHT_NO.', 7: 'ADDRESS', 8: 'REPORTING_LOCATION', 9: 'LANDMARK', 10: 'PASSENGER_MOBILE'}

        df_h = df[is_header].rename(columns=h_map)
        df_p = df[is_passenger].rename(columns=p_map)
        
        cols_h = [c for c in h_map.values() if c in df_h.columns] + ['Trip_ID']
        cols_p = [c for c in p_map.values() if c in df_p.columns] + ['Trip_ID']
        
        merged = pd.merge(df_p[cols_p], df_h[cols_h], on='Trip_ID', how='left')

        # Cleaning Logic
        for col in ['TRIP_ID', 'Trip_ID']:
            if col in merged.columns:
                merged[col] = merged[col].astype(str).str.replace('T', '', regex=False)
                merged.rename(columns={col: 'TRIP_ID'}, inplace=True)

        if 'AGENCY_NAME' in merged.columns:
            merged['AGENCY_NAME'] = merged['AGENCY_NAME'].apply(lambda x: "UNITED FACILITIES" if "UNITED FACILITIES" in str(x).upper() else x)

        if 'VEHICLE_NO' in merged.columns:
            merged['VEHICLE_NO'] = merged['VEHICLE_NO'].astype(str).str.replace('-', '', regex=False)

        if 'D_LOGIN' in merged.columns:
            login = merged['D_LOGIN'].astype(str).str.strip().str.split(' ', n=1, expand=True)
            if len(login.columns) > 0: merged['DIRECTION'] = login[0].str.upper().replace({'LOGIN': 'PICKUP', 'LOGOUT': 'DROP'})
            if len(login.columns) > 1: merged['SHIFT_TIME'] = login[1]

        # Explicit Removal
        cols_to_remove = ['PAX_NO', 'D_LOGIN', 'MARSHALL', 'DISTANCE', 'EMP_COUNT', 'TRIP_COUNT']
        merged = merged.drop(columns=cols_to_remove, errors='ignore')

        for col in merged.select_dtypes(include=['object']):
            merged[col] = merged[col].astype(str).str.upper().str.strip()

        return merged
    except: return pd.DataFrame()

def process_raw_data(file_list_bytes):
    all_dfs = []
    for _, content in file_list_bytes:
        try:
            df_raw = pd.read_excel(io.BytesIO(content), header=None).dropna(how="all").reset_index(drop=True)
            cleaned = _clean_single_raw_df(df_raw)
            if not cleaned.empty: all_dfs.append(cleaned)
        except: pass

    if not all_dfs: return None, None, None
    final_df = pd.concat(all_dfs, ignore_index=True)

    # Input Map -> DB Columns
    DB_MAP = {
        'TRIP_DATE': 'shift_date', 'TRIP_ID': 'trip_id', 'AGENCY_NAME': 'vendor', 
        'FLIGHT_NO.': 'flight_number', 'EMPLOYEE_ID': 'employee_id', 'EMPLOYEE_NAME': 'employee_name', 
        'GENDER': 'gender', 'EMP_CATEGORY': 'emp_category', 'ADDRESS': 'address', 
        'PASSENGER_MOBILE': 'passenger_mobile', 'LANDMARK': 'landmark', 'VEHICLE_NO': 'cab_reg_no',
        'DRIVER_NAME': 'driver_name', 'DRIVER_MOBILE': 'driver_mobile', 'DIRECTION': 'trip_direction',
        'SHIFT_TIME': 'shift_time', 'REPORTING_TIME': 'pickup_time', 'REPORTING_LOCATION': 'office'    
    }

    final_db = final_df.rename(columns=DB_MAP).fillna("")
    
    # 2. Date/Time Formatting
    if 'shift_date' in final_db.columns:
        final_db['shift_date'] = pd.to_datetime(final_db['shift_date'], errors='coerce').dt.strftime('%d-%m-%Y')
        final_db['trip_date'] = final_db['shift_date'] # Duplicate to trip_date
    
    if 'shift_time' in final_db.columns:
        final_db['shift_time'] = pd.to_datetime(final_db['shift_time'], errors='coerce', format='mixed').dt.strftime('%H:%M')

    # 3. Standardize (Matches RawTripData Model)
    final_db = standardize_dataframe(final_db)
    if final_db is None: return None, None, None

    return create_styled_excel(final_db, "Raw_Cleaned")


# ==========================================
# 3. OPERATION DATA CLEANER
# ==========================================
def process_operation_data(file_list_bytes):
    # Mapping specifically for reading operation files
    READ_MAP = {
        'DATE': 'Shift Date', 'TRIP ID': 'Trip ID', 'FLT NO.': 'Flight Number', 'SAP ID': 'Employee ID',
        'EMP NAME': 'Employee Name', 'EMPLOYEE ADDRESS': 'Address', 'PICKUP LOCATION': 'Landmark',
        'DROP LOCATION': 'drop_location', 'CAB NO': 'Cab Reg No', 'AIRPORT DROP': 'Shift Time', 
        'GUARD ROUTE': 'guard_route', 'MARSHALL': 'guard_route', 'REMARKS': 'MiS Remark'
    }
    SKIP_HEADERS = ['PICKUP TIME', 'CONTACT NO', 'CONTACT NO.']

    wb = Workbook()
    ws = wb.active
    ws.title = "Operation_Data"
    
    # Styles
    header_fill = PatternFill(start_color="0070C0", end_color="0070C0", fill_type="solid")
    header_font = Font(size=11, bold=True, color="FFFFFF")
    align_center = Alignment(horizontal='center', vertical='center')
    border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))

    # Write Mandatory Headers
    for col_idx, header in enumerate(MANDATORY_HEADERS, 1):
        cell = ws.cell(row=1, column=col_idx, value=header)
        cell.fill = header_fill; cell.font = header_font; cell.alignment = align_center; cell.border = border
        ws.column_dimensions[cell.column_letter].width = 15

    target_row = 2
    files_processed = 0
    data_rows = [] 
    
    # Track extra headers found dynamically
    extra_headers_map = {} 
    next_extra_col_idx = len(MANDATORY_HEADERS) + 1

    for filename, content in file_list_bytes:
        if not filename.lower().endswith('.xls'): continue
        try:
            rb = xlrd.open_workbook(file_contents=content, formatting_info=True)
            rs = rb.sheet_by_index(0)
            
            # Map Source Columns
            source_headers = [str(rs.cell_value(0, c)).strip().upper() for c in range(rs.ncols)]
            col_to_target_map = {} 
            
            for idx, raw_header in enumerate(source_headers):
                if any(skip in raw_header for skip in SKIP_HEADERS): continue
                
                match = next((val for key, val in READ_MAP.items() if key in raw_header), None)
                if match:
                    col_to_target_map[idx] = {'type': 'mandatory', 'name': match}
                else:
                    clean_name = raw_header
                    if clean_name not in extra_headers_map:
                        extra_headers_map[clean_name] = next_extra_col_idx
                        # Add header to Excel
                        cell = ws.cell(row=1, column=next_extra_col_idx, value=clean_name)
                        cell.fill = header_fill; cell.font = header_font; cell.alignment = align_center; cell.border = border
                        ws.column_dimensions[cell.column_letter].width = 15
                        next_extra_col_idx += 1
                    col_to_target_map[idx] = {'type': 'extra', 'name': clean_name}

            start_row = 0 if files_processed == 0 else 1 
            
            for r_idx in range(1, rs.nrows):
                row_vals = rs.row_values(r_idx)
                # Skip empty/near empty rows
                if sum(1 for v in row_vals if str(v).strip() != "") <= 2: continue
                
                row_data_map = {} 
                db_row_dict = {}  
                
                for c_idx in range(rs.ncols):
                    if c_idx not in col_to_target_map: continue
                    mapping = col_to_target_map[c_idx]
                    target_header = mapping['name']
                    
                    val = rs.cell_value(r_idx, c_idx)
                    bg, fg, bold = get_xls_style_data(rb, rs.cell_xf_index(r_idx, c_idx))
                    
                    style_data = {'val': val, 'bg': bg, 'fg': fg, 'bold': bold}
                    
                    if mapping['type'] == 'mandatory':
                         row_data_map[target_header] = style_data
                         if target_header in MANDATORY_DB_MAP:
                             db_row_dict[MANDATORY_DB_MAP[target_header]] = val
                         elif target_header == 'guard_route':
                             db_row_dict['guard_route'] = val
                         elif target_header == 'drop_location':
                             db_row_dict['drop_location'] = val
                    else:
                        row_data_map[f"EXTRA_{target_header}"] = style_data
                        db_row_dict[target_header] = val # Store extra in DB dict too if needed by model

                # Write Row to Excel
                ws.row_dimensions[target_row].height = 25
                
                # Write Mandatory
                for c_out, m_header in enumerate(MANDATORY_HEADERS, 1):
                    cell = ws.cell(row=target_row, column=c_out)
                    if m_header in row_data_map:
                        data = row_data_map[m_header]
                        cell.value = data['val']
                        if data['bg']: cell.fill = PatternFill(start_color=data['bg'], end_color=data['bg'], fill_type='solid')
                        cell.font = Font(size=11, bold=data['bold'], color=data['fg'] if data['fg'] else None)
                    else: cell.value = ""
                    cell.alignment = align_center; cell.border = border
                
                # Write Extra
                for extra_name, extra_col_idx in extra_headers_map.items():
                    key = f"EXTRA_{extra_name}"
                    cell = ws.cell(row=target_row, column=extra_col_idx)
                    if key in row_data_map:
                        data = row_data_map[key]
                        cell.value = data['val']
                        if data['bg']: cell.fill = PatternFill(start_color=data['bg'], end_color=data['bg'], fill_type='solid')
                        cell.font = Font(size=11, bold=data['bold'], color=data['fg'] if data['fg'] else None)
                    else: cell.value = ""
                    cell.alignment = align_center; cell.border = border
                
                data_rows.append(db_row_dict)
                target_row += 1
            
            files_processed += 1
            rb.release_resources()
        except Exception as e: print(f"Error {filename}: {e}")

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    
    if not data_rows: return None, output, "Operation_Cleaned.xlsx"

    df_db = pd.DataFrame(data_rows)

    # Post-Process Dates/Times
    if 'shift_date' in df_db.columns: 
        df_db['shift_date'] = pd.to_datetime(df_db['shift_date'], errors='coerce').dt.strftime('%d-%m-%Y')
    if 'shift_time' in df_db.columns: 
        df_db['shift_time'] = pd.to_datetime(df_db['shift_time'], errors='coerce', format='mixed').dt.strftime('%H:%M')
    if 'guard_route' in df_db.columns:
        df_db['guard_route'] = df_db['guard_route'].astype(str).apply(lambda x: 'Guard' if 'marshall' in x.lower() else '')
        # Map internal 'guard_route' to 'route_status' or 'marshall' field if strictly required
        # Note: OperationData Model uses 'route_status', but raw 'marshall' map logic existed. 
        # Standardize will handle missing cols, but we populated 'guard_route' in dict.

    # Standardize (Matches OperationData Model)
    df_db = standardize_dataframe(df_db)
    return df_db, output, "Operation_Cleaned.xlsx"
# ==========================================
# 4. Fastag Data Cleaner
# ==========================================
def process_fastag_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process Fastag data to match the FastagData model.
    """
    # Standardize column names to match model
    df = df.rename(columns={
        'fastag_id': 'fastag_id',
        'vehicle_number': 'vehicle_number',
        'time': 'time',
       
    })



