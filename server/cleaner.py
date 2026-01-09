import pandas as pd
import numpy as np
import io
import xlrd
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side

# ==========================================
# 1. CLIENT DATA CLEANER
# ==========================================
def process_client_data(file_content):
    try:
        # Mapping: Excel Header -> DB Column Name
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
        
        # Filter & Rename for DB
        df_db = df[list(COL_MAP.keys())].rename(columns=COL_MAP).fillna("")
        
        # Generate Unique ID
        if "trip_id" in df_db.columns and "employee_id" in df_db.columns:
            df_db["unique_id"] = df_db["trip_id"].str.strip() + df_db["employee_id"].str.strip()
            df_db = df_db[~df_db["unique_id"].str.contains("nan|None", case=False, na=False)]
            df_db = df_db[df_db["unique_id"] != ""]
        else:
            return None, None, None

        # --- EXCEL GENERATION ---
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # Write original columns (user friendly names) to Excel
            df_excel = df[list(COL_MAP.keys())].fillna("")
            df_excel.to_excel(writer, sheet_name='Client_Data', index=False)
            
            workbook = writer.book
            worksheet = writer.sheets['Client_Data']
            header_fmt = workbook.add_format({'bold': True, 'bg_color': '#0070C0', 'font_color': 'white', 'border': 1})
            for i, col in enumerate(df_excel.columns):
                worksheet.write(0, i, col, header_fmt)
                worksheet.set_column(i, i, 20)

        output.seek(0)
        return df_db, output, "Client_Cleaned.xlsx"
    except Exception as e:
        print(f"❌ Client Cleaner Error: {e}")
        return None, None, None

# ==========================================
# 2. RAW DATA CLEANER
# ==========================================
def _clean_single_raw_df(df):
    try:
        df["Trip_ID"] = np.where(df.iloc[:, 10].astype(str).str.startswith("T"), df.iloc[:, 10], np.nan)
        df["Trip_ID"] = df["Trip_ID"].ffill()

        is_header = df.iloc[:, 1].astype(str).str.contains("UNITED FACILITIES", na=False)
        is_passenger = df.iloc[:, 0].astype(str).str.match(r"^[1-5]$")

        # Map to INTERMEDIATE names first
        h_map = {0: 'TRIP_DATE', 1: 'AGENCY_NAME', 2: 'D_LOGIN', 3: 'VEHICLE_NO', 4: 'DRIVER_NAME', 6: 'DRIVER_MOBILE', 7: 'MARSHALL', 8: 'DISTANCE', 9: 'EMP_COUNT', 10: 'TRIP_COUNT'}
        p_map = {0: 'PAX_NO', 1: 'REPORTING_TIME', 2: 'EMPLOYEE_ID', 3: 'EMPLOYEE_NAME', 4: 'GENDER', 5: 'EMP_CATEGORY', 6: 'FLIGHT_NO.', 7: 'ADDRESS', 8: 'REPORTING_LOCATION', 9: 'LANDMARK', 10: 'PASSENGER_MOBILE'}

        df_h = df[is_header].rename(columns=h_map)
        df_p = df[is_passenger].rename(columns=p_map)
        
        # Filter valid columns
        df_h = df_h[[c for c in h_map.values() if c in df_h.columns] + ['Trip_ID']]
        df_p = df_p[[c for c in p_map.values() if c in df_p.columns] + ['Trip_ID']]

        merged_df = pd.merge(df_p, df_h, on='Trip_ID', how='left')

        # Cleanup
        if 'TRIP_ID' in merged_df.columns:
            merged_df['TRIP_ID'] = merged_df['TRIP_ID'].astype(str).str.replace('T', '', regex=False)
        else:
            merged_df['TRIP_ID'] = merged_df['Trip_ID'].astype(str).str.replace('T', '', regex=False)

        if 'VEHICLE_NO' in merged_df.columns:
            merged_df['VEHICLE_NO'] = merged_df['VEHICLE_NO'].astype(str).str.replace('-', '', regex=False)

        if 'D_LOGIN' in merged_df.columns:
            login = merged_df['D_LOGIN'].astype(str).str.strip().str.split(' ', n=1, expand=True)
            if len(login.columns) > 0: merged_df['DIRECTION'] = login[0].str.upper().str.replace('LOGIN', 'PICKUP').str.replace('LOGOUT', 'DROP')
            if len(login.columns) > 1: merged_df['SHIFT_TIME'] = login[1]

        # Uppercase
        for col in merged_df.select_dtypes(include=['object']):
            merged_df[col] = merged_df[col].astype(str).str.upper().str.strip()

        return merged_df
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

    # Map to DB Columns
    DB_MAP = {
        'TRIP_DATE': 'trip_date', 'TRIP_ID': 'trip_id', 'AGENCY_NAME': 'agency_name', 'FLIGHT_NO.': 'flight_no',
        'EMPLOYEE_ID': 'employee_id', 'EMPLOYEE_NAME': 'employee_name', 'GENDER': 'gender', 'EMP_CATEGORY': 'emp_category',
        'ADDRESS': 'address', 'PASSENGER_MOBILE': 'passenger_mobile', 'LANDMARK': 'landmark', 'VEHICLE_NO': 'vehicle_no',
        'DRIVER_NAME': 'driver_name', 'DRIVER_MOBILE': 'driver_mobile', 'DISTANCE': 'distance', 'DIRECTION': 'direction',
        'SHIFT_TIME': 'shift_time', 'REPORTING_TIME': 'reporting_time', 'REPORTING_LOCATION': 'reporting_location',
        'EMP_COUNT': 'emp_count', 'PAX_NO': 'pax_no', 'MARSHALL': 'marshall', 'TRIP_COUNT': 'trip_count'
    }

    # Rename for DB
    final_db = final_df.rename(columns=DB_MAP)
    valid_db_cols = [c for c in DB_MAP.values() if c in final_db.columns]
    final_db = final_db[valid_db_cols].fillna("")

    # Unique ID
    if "trip_id" in final_db.columns and "employee_id" in final_db.columns:
        final_db["unique_id"] = final_db["trip_id"] + final_db["employee_id"]
        final_db = final_db[~final_db["unique_id"].str.contains("nan|None", case=False, na=False)]
    else:
        final_db["unique_id"] = ""

    # Excel Output (User friendly headers)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        final_df.to_excel(writer, index=False, sheet_name='Raw_Data')
        workbook = writer.book
        worksheet = writer.sheets['Raw_Data']
        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#0070C0', 'font_color': 'white'})
        for i, col in enumerate(final_df.columns):
            worksheet.write(0, i, col, header_fmt)
            worksheet.set_column(i, i, 15)

    output.seek(0)
    return final_db, output, "Raw_Cleaned.xlsx"

# ==========================================
# 3. OPERATION DATA CLEANER (Mapping Logic + Styles)
# ==========================================
def get_xls_styles(rs, r_idx, c_idx):
    try:
        xf = rs.book.xf_list[rs.cell_xf_index(r_idx, c_idx)]
        bg_fill = None
        if xf.background.pattern_colour_index < 64:
            rgb = rs.book.colour_map.get(xf.background.pattern_colour_index)
            if rgb: 
                hex_val = '%02x%02x%02x' % rgb
                bg_fill = PatternFill(start_color=hex_val, end_color=hex_val, fill_type='solid')
        
        xls_font = rs.book.font_list[xf.font_index]
        font_hex = None
        if xls_font.colour_index < 64:
            rgb = rs.book.colour_map.get(xls_font.colour_index)
            if rgb: font_hex = '%02x%02x%02x' % rgb
        
        return bg_fill, Font(size=13, bold=bool(xls_font.bold), color=font_hex)
    except: return None, Font(size=13)

def process_operation_data(file_list_bytes):
    # 1. STYLE PRESERVATION MERGE (For Excel Download)
    wb = Workbook()
    ws = wb.active
    ws.title = "Operation_Data"
    
    header_fill = PatternFill(start_color="0070C0", end_color="0070C0", fill_type="solid")
    header_font = Font(size=13, bold=True, color="FFFFFF")
    align_center = Alignment(horizontal='center', vertical='center')
    border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))

    target_row = 1
    files_processed = 0
    
    # 2. DATA EXTRACTION (For Database)
    all_dfs = []

    for filename, content in file_list_bytes:
        if not filename.lower().endswith('.xls'): continue
        try:
            # A. Style Processing
            rb = xlrd.open_workbook(file_contents=content, formatting_info=True)
            rs = rb.sheet_by_index(0)
            start_row = 0 if files_processed == 0 else 1
            
            for r_idx in range(start_row, rs.nrows):
                row_vals = rs.row_values(r_idx)
                if all(v in (None, "", " ") for v in row_vals): continue
                ws.row_dimensions[target_row].height = 30
                
                for c_idx in range(rs.ncols):
                    val = rs.cell_value(r_idx, c_idx)
                    cell = ws.cell(row=target_row, column=c_idx + 1, value=val)
                    cell.border = border
                    
                    if target_row == 1:
                        cell.fill, cell.font, cell.alignment = header_fill, header_font, align_center
                    else:
                        fill, font = get_xls_styles(rs, r_idx, c_idx)
                        if fill: cell.fill = fill
                        cell.font = font
                        cell.alignment = align_center
                target_row += 1
            files_processed += 1
            
            # B. Data Extraction for DB
            df_temp = pd.read_excel(io.BytesIO(content), header=0, engine="xlrd")
            all_dfs.append(df_temp)
            
        except: pass

    # Save Excel
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)

    # Process Data for DB
    if not all_dfs: return None, output, "Operation_Cleaned.xlsx"
    
    combined = pd.concat(all_dfs, ignore_index=True)
    combined.columns = combined.columns.astype(str).str.strip().str.upper()

    # DB Mapping
    DB_MAP = {
        'DATE': 'date', 'TRIP ID': 'trip_id', 'FLT NO.': 'flight_number', 'SAP ID': 'employee_id',
        'EMP NAME': 'employee_name', 'EMPLOYEE ADDRESS': 'address', 'PICKUP LOCATION': 'pickup_location',
        'DROP LOCATION': 'drop_location', 'CAB NO': 'cab_no', 'AIRPORT DROP': 'shift_time', 
        'GUARD ROUTE': 'guard_route', 'REMARKS': 'remarks'
    }

    df_db = pd.DataFrame()
    for src, target in DB_MAP.items():
        match = next((c for c in combined.columns if src in c), None)
        if match: df_db[target] = combined[match]
        else: df_db[target] = ""
    
    # Format & ID
    if 'date' in df_db.columns:
        df_db['date'] = pd.to_datetime(df_db['date'], errors='coerce').dt.strftime('%d-%m-%Y')
    
    if 'trip_id' in df_db.columns and 'employee_id' in df_db.columns:
        df_db['unique_id'] = df_db['trip_id'].astype(str) + df_db['employee_id'].astype(str)
        df_db = df_db[~df_db['unique_id'].str.contains("nan|None", case=False, na=False)]
    else:
        return None, output, "Operation_Cleaned.xlsx"

    return df_db.fillna(""), output, "Operation_Cleaned.xlsx"