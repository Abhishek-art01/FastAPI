import pandas as pd

df = pd.read_csv(
    r"D:\my_projects\air-india-data\data-jan-2026\ba_row_data\01-01-2026-18-01-2026New_RBD_d066.csv",
    low_memory=False
)

# -----------------------------
# Row filter
# -----------------------------
df = df[df["Trip Id"] != 0]

# -----------------------------
# Shift Time from Trip Type
# -----------------------------
df["Shift Time"] = df["Trip Type"].astype(str)
df.loc[
    df["Shift Time"].str.contains("LOGIN|LOGOUT", case=False, na=False),
    "Shift Time"
] = "00:00"

# -----------------------------
# Trip Direction
# -----------------------------
df["Trip Direction"] = df["Direction"].str.upper().map({
    "LOGIN": "PICKUP",
    "LOGOUT": "DROP"
})

# -----------------------------
# Pickup / Drop
# -----------------------------
df["Pickup Time"] = df["Duty Start"]
df["Drop Time"] = df["Duty End"]

# -----------------------------
# Location logic
# -----------------------------
df["Airport Name"] = df.apply(
    lambda x: x["Start Location Address"]
    if x["Trip Direction"] == "DROP"
    else x["End Location Address"],
    axis=1
)

df["Address"] = df.apply(
    lambda x: x["Start Location Address"]
    if x["Trip Direction"] == "PICKUP"
    else x["End Location Address"],
    axis=1
)

df["Landmark"] = df.apply(
    lambda x: x["Start Location Landmark"]
    if x["Trip Direction"] == "PICKUP"
    else x["End Location Landmark"],
    axis=1
)

# -----------------------------
# Trip Date
# -----------------------------
df["Trip Date"] = df["Leg Date"].astype(str) + " " + df["Shift Time"]

# -----------------------------
# Static / blanks
# -----------------------------
df["Employee ID"] = ""
df["Gender"] = ""
df["EMP_CATEGORY"] = ""
df["Employee Name"] = ""
df["Cab Type"] = ""
df["Flight Number"] = ""
df["Flight Category"] = ""
df["Flight Route"] = ""
df["Flight Type"] = ""
df["UNA"] = ""
df["UNA2"] = ""
df["Route Status"] = ""
df["Clubbing Status"] = ""
df["GPS TIME"] = ""
df["GPS REMARK"] = ""
df["In App/ Extra"] = "BA Row Data"

# -----------------------------
# Remarks
# -----------------------------
df["BA REMARK"] = df["Trip Status"]
df["MiS Remark"] = df["Comments"]

# -----------------------------
# Final column order
# -----------------------------
final_columns = [
    "Leg Date", "Trip Id", "Employee ID", "Gender", "EMP_CATEGORY", "Employee Name",
    "Shift Time", "Pickup Time", "Drop Time", "Trip Direction",
    "Registration", "Cab Type", "Vendor", "Office",
    "Airport Name", "Landmark", "Address",
    "Flight Number", "Flight Category", "Flight Route", "Flight Type",
    "Trip Date", "MiS Remark", "In App/ Extra", "Traveled Employee Count", "UNA2", "UNA",
    "BA REMARK", "Route Status", "Clubbing Status", "GPS TIME", "GPS REMARK", "Billing Zone Name",
    "Leg Type", "Trip Source", "Trip Type", "Leg Start", "Leg End", "Audit Results", "Audit Done By", "Trip Audited"
]

final_df = df[final_columns]

final_df.to_csv(r"D:\my_projects\air-india-data\data-jan-2026\ba_row_data\final_output2.csv", index=False)

print("Final BA CSV generated successfully.")



