# server/cleaner.py
import pandas as pd
import io
import numpy as np
from datetime import timedelta

# --- EXCEL GENERATION HELPERS ---
def to_excel_billing(df):
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    workbook = writer.book
    worksheet = workbook.add_worksheet('Sheet1')
    
    header_fmt = workbook.add_format({'bold': True, 'fg_color': '#0070C0', 'font_color': '#FFFFFF', 'border': 1})
    base_fmt = workbook.add_format({'border': 1, 'text_wrap': True})
    
    for col_num, value in enumerate(df.columns):
        worksheet.write(0, col_num, value, header_fmt)
        worksheet.set_column(col_num, col_num, 20) 

    for row_idx, row in df.iterrows():
        for col_idx, value in enumerate(row):
            val = value if pd.notna(value) else ""
            worksheet.write(row_idx + 1, col_idx, val, base_fmt)
            
    writer.close()
    output.seek(0)
    return output

def to_excel_operations(df):
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    workbook = writer.book
    worksheet = workbook.add_worksheet('Sheet1')
    
    header_fmt = workbook.add_format({'bold': True, 'fg_color': '#0070C0', 'font_color': '#FFFFFF', 'border': 1})
    base_fmt = workbook.add_format({'border': 1, 'text_wrap': True})
    
    width_map = {'ADDRESS': 50, 'EMPLOYEE_NAME': 25, 'LANDMARK': 20}
    
    for col_num, value in enumerate(df.columns):
        worksheet.write(0, col_num, value, header_fmt)
        width = width_map.get(str(value).upper(), 15)
        worksheet.set_column(col_num, col_num, width)

    for row_idx, row in df.iterrows():
        for col_idx, value in enumerate(row):
            val = value if pd.notna(value) else ""
            worksheet.write(row_idx + 1, col_idx, val, base_fmt)

    writer.close()
    output.seek(0)
    return output

# --- PROCESSING LOGIC ---
def process_dataframe(file_content):
    try:
        # Try reading as standard Excel
        df = pd.read_excel(io.BytesIO(file_content), header=None)
        print("✅ Excel file loaded successfully.")
    except Exception as e:
        # 👇 THIS PRINT IS CRITICAL FOR DEBUGGING
        print(f"❌ ERROR READING EXCEL: {e}") 
        return None, None, None

    df.drop(index=1, inplace=True)
    df.dropna(how="all", inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Extract Trip ID
    df["Trip_ID"] = df[10].astype(str).apply(lambda x: x if str(x).startswith("T") else "")
    df["Trip_ID"] = df["Trip_ID"].replace("", pd.NA).ffill()

    # Split
    df_headers = df[df[1].astype(str).str.contains("UNITED FACILITIES", na=False)].copy()
    df_passengers = df[df[0].astype(str).str.match(r"^[12345]$")].copy()
    
    # Rename
    header_map = {0: 'Trip_Date', 1: 'Agency_Name', 2: 'Driver_Login_Time', 3: 'Vehicle_No', 4: 'Driver_Name', 5: 'Trip_Zone', 6: 'Driver_Mobile', 7: 'Marshall', 8: 'Distance', 9: 'Emp_Count', 10: 'Trip_Count'}
    pass_map = {0: 'Pax_no', 1: 'Reporting_Time', 2: 'Employee_ID', 3: 'Employee_Name', 4: 'Gender', 5: 'Emp_Category', 6: 'Flight_No.', 7: 'Address', 8: 'Reporting_Location', 9: 'Landmark', 10: 'Passenger_Mobile'}
    
    df_h = df_headers.rename(columns=header_map)
    df_p = df_passengers.rename(columns=pass_map)
    
    # Merge
    final = pd.merge(df_p, df_h, on='Trip_ID', how='left')
    
    # Clean Data
    final['Trip_ID'] = final['Trip_ID'].astype(str).str.replace('T', '', regex=False)
    final['Vehicle_No'] = final['Vehicle_No'].astype(str).str.replace('-', '', regex=False)
    
    split = final['Driver_Login_Time'].astype(str).str.split(' ', n=1, expand=True)
    final['Direction'] = split[0].str.replace('Login', 'Pickup').str.replace('Logout', 'Drop')
    
    # Time Calc
    final['Shift_Time_Obj'] = pd.to_datetime(split[1], format='%H:%M', errors='coerce')
    final['Shift_Time'] = final['Shift_Time_Obj'].dt.strftime('%H:%M')
    
    # Date Format
    final['Trip_Date'] = pd.to_datetime(final['Trip_Date'], errors='coerce').dt.strftime('%d-%m-%Y')
    
    # Upper Case
    final.columns = final.columns.astype(str).str.upper()
    str_cols = final.select_dtypes(include=['object']).columns
    final[str_cols] = final[str_cols].apply(lambda x: x.astype(str).str.strip().str.upper())

    # --- BILLING DF ---
    b_cols = ['TRIP_DATE', 'TRIP_ID', 'FLIGHT_NO.', 'EMPLOYEE_ID', 'EMPLOYEE_NAME', 'GENDER', 'ADDRESS', 'PASSENGER_MOBILE', 'LANDMARK', 'VEHICLE_NO', 'DIRECTION', 'SHIFT_TIME', 'EMP_COUNT', 'PAX_NO', 'MARSHALL', 'REPORTING_LOCATION']
    b_cols = [c for c in b_cols if c in final.columns]
    billing_df = final[b_cols].copy()

    # --- OPS DF ---
    ops_df = final.copy()
    if 'SHIFT_TIME_OBJ' in ops_df.columns:
        ops_df['PICKUP POINT'] = (ops_df['SHIFT_TIME_OBJ'] - timedelta(hours=2)).dt.strftime('%H:%M')
    else:
        ops_df['PICKUP POINT'] = ""
        
    ops_cols = ['TRIP_DATE', 'TRIP_ID', 'FLIGHT_NO.', 'EMPLOYEE_ID', 'EMPLOYEE_NAME', 'ADDRESS', 'LANDMARK', 'REPORTING_LOCATION', 'PASSENGER_MOBILE', 'VEHICLE_NO', 'DIRECTION', 'PICKUP POINT', 'SHIFT_TIME', 'MARSHALL']
    ops_cols = [c for c in ops_cols if c in ops_df.columns]
    ops_df = ops_df[ops_cols]

    # Format Ops (Groups)
    ops_list = []
    empty = pd.DataFrame([[np.nan]*len(ops_df.columns)], columns=ops_df.columns)
    header = pd.DataFrame([ops_df.columns.values], columns=ops_df.columns)
    
    groups = list(ops_df.groupby('TRIP_ID', sort=False))
    for i, (tid, grp) in enumerate(groups):
        ops_list.append(grp)
        if i < len(groups) - 1:
            ops_list.append(empty)
            ops_list.append(header)
            
    ops_final = pd.concat(ops_list, ignore_index=True)
    
    base_name = f"{final['TRIP_DATE'].iloc[0]} {final['DIRECTION'].iloc[0]}"
    return billing_df, ops_final, base_name