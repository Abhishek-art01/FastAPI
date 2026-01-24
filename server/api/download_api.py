# ==========================================
# 4. UNIVERSAL DOWNLOAD ENDPOINTS
# ==========================================
@app.get("/operation-manager")
async def operation_manager_page(request: Request):
    if not request.session.get("user"): return RedirectResponse(url="/login", status_code=303)
    return templates["operation-manager"].TemplateResponse("operation_manager.html", {"request": request})

@app.get("/download/{filename}")
async def download_file(filename: str, request: Request):
    if not request.session.get("user"):
        return Response("Unauthorized", status_code=401)
    
    file_path = DIRS["cleaner"] / "generated" / filename
    if not file_path.exists():
        return Response("File not found", status_code=404)
        
    return FileResponse(
        path=file_path, 
        filename=filename, 
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
@app.get("/api/{table_type}/download")
def download_specific_table(table_type: str, session: Session = Depends(get_session)):
    model_map = {
        "operation": OperationData,
        "client": ClientData,
        "raw": RawTripData,
        "trip_data": TripData
    }
    
    if table_type not in model_map:
        return {"status": "error", "message": "Invalid table type selected."}
    
    model_class = model_map[table_type]
    statement = select(model_class)
    results = session.exec(statement).all()
    
    if not results:
        return {"status": "error", "message": f"No data found in {table_type} table."}
    
    data = [row.model_dump() for row in results]
    df = pd.DataFrame(data)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Report')
    output.seek(0)
    
    filename = f"{table_type.capitalize()}_Export.xlsx"
    headers = {'Content-Disposition': f'attachment; filename="{filename}"'}
    return StreamingResponse(
        output, 
        headers=headers, 
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

@app.post("/api/operation/upload")
async def upload_operation_data(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))
        
        save_path = CLIENT_DIR / "OperationManager" / "processed_db_mock.csv"
        # Ensure dir exists
        os.makedirs(save_path.parent, exist_ok=True)
        df.to_csv(save_path, index=False)

        return JSONResponse(
            content={
                "status": "success", 
                "message": f"Successfully processed {len(df)} rows and updated Database."
            }
        )
    except Exception as e:
        print(f"Error processing file: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)}
        )

