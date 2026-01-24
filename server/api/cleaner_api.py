# ==========================================
# 🚀 DATA CLEANER API 
# ==========================================
@app.get("/cleaner")
async def cleaner_page(request: Request):
    if not request.session.get("user"): return RedirectResponse(url="/login", status_code=303)
    return templates["cleaner"].TemplateResponse("Datacleaner.html", {"request": request, "user": request.session.get("user")})


@app.post("/clean-data")
async def clean_data(
    files: List[UploadFile] = File(...),
    cleanerType: str = Form(...),
    session: Session = Depends(get_session)
):
    try:
        print(f"🚀 Processing {len(files)} files with mode: {cleanerType}")
        df_result = None
        excel_output = None
        filename = "output.xlsx"
        rows_saved = 0
        new_addresses = 0


        # ==========================================
        # A. CLIENT DATA
        # ==========================================
        if cleanerType == "client":
            content = await files[0].read()
            df_result, excel_output, filename = process_client_data(content)
            
            # 1. Database Logic
            rows_saved = bulk_save_unique(session, ClientData, df_result)
            if df_result is not None:
                new_addresses = sync_addresses_to_t3(session, df_result)
            
            # 2. Sync Unique IDs (Custom Logic)
            if df_result is not None and not df_result.empty and "unique_id" in df_result.columns:
                incoming_ids = df_result["unique_id"].dropna().unique().tolist()
                existing_ids = set(session.exec(select(ClientData.unique_id).where(col(ClientData.unique_id).in_(incoming_ids))).all())
                new_rows = df_result[~df_result["unique_id"].isin(existing_ids)]
                if not new_rows.empty:
                    records = [ClientData(**row.to_dict()) for _, row in new_rows.iterrows()]
                    session.add_all(records)
                    session.commit()

            # 3. Save & Return (FIXED: Added this block)
            if excel_output is None:
                return Response("Error processing Client data", status_code=400)
            
            generated_dir = DIRS["cleaner"] / "generated"
            os.makedirs(generated_dir, exist_ok=True)
            save_path = generated_dir / filename
            with open(save_path, "wb") as f:
                f.write(excel_output.read())

            return {
                "status": "success", 
                "file_url": filename, 
                "rows_processed": len(df_result) if df_result is not None else 0, 
                "db_rows_added": rows_saved, 
                "new_addresses_added": new_addresses
            }

        # ==========================================
        # B. RAW DATA
        # ==========================================
        elif cleanerType == "raw":
            file_data = []
            for f in files:
                content = await f.read()
                file_data.append((f.filename, content))
            
            df_result, excel_output, filename = process_raw_data(file_data)
            
            # 1. Database Logic
            rows_saved = bulk_save_unique(session, RawTripData, df_result)
            if df_result is not None:
                new_addresses = sync_addresses_to_t3(session, df_result)

            # 2. Save & Return (FIXED: Added this block)
            if excel_output is None:
                return Response("Error processing Raw data", status_code=400)
            
            generated_dir = DIRS["cleaner"] / "generated"
            os.makedirs(generated_dir, exist_ok=True)
            save_path = generated_dir / filename
            with open(save_path, "wb") as f:
                f.write(excel_output.read())

            return {
                "status": "success", 
                "file_url": filename, 
                "rows_processed": len(df_result) if df_result is not None else 0, 
                "db_rows_added": rows_saved, 
                "new_addresses_added": new_addresses
            }
        # --- C. OPERATION ---            

        elif cleanerType == "operation":
            file_data = []
            for f in files:
                content = await f.read()
                file_data.append((f.filename, content))
            df_result, excel_output, filename = process_operation_data(file_data)

            if excel_output is None:
                return Response("Error processing data", status_code=400)

            generated_dir = DIRS["cleaner"] / "generated"
            os.makedirs(generated_dir, exist_ok=True)
            save_path = generated_dir / filename
            with open(save_path, "wb") as f:
                f.write(excel_output.read())

            row_count = len(df_result) if df_result is not None else "Formatting Only"
            return {
                "status": "success",
                "file_url": filename,
                "rows_processed": row_count,
                "db_rows_added": rows_saved
            }

        # --- D. BA ROW DATA (CSV) ---
        elif cleanerType == "ba_row":
            content = await files[0].read()
            df_result, excel_output, filename = process_ba_row_data(content)
            
            # ... (Existing Database Logic) ...

            # ✅ ADD THIS BLOCK TO SAVE THE FILE AND RETURN RESPONSE
            if excel_output is None:
                return Response("Error processing BA data", status_code=400)

            # 1. Save the generated file to disk so the frontend can download it
            generated_dir = DIRS["cleaner"] / "generated"  # Ensure you have DIRS defined or use a hardcoded path
            os.makedirs(generated_dir, exist_ok=True)
            save_path = generated_dir / filename
            
            with open(save_path, "wb") as f:
                f.write(excel_output.read())

            # 2. Return the JSON response the frontend is waiting for
            row_count = len(df_result) if df_result is not None else 0
            return {
                "status": "success",
                "file_url": filename,
                "rows_processed": row_count,
                "db_rows_added": rows_saved
            }

        # --- E. FASTAG DATA (PDF) ---
        elif cleanerType == "fastag":
            # 1. Collect files as (filename, content) tuples
            file_data = []
            for f in files:
                content = await f.read()
                file_data.append((f.filename, content))  # <--- Pass filename here!
            
            # 2. Pass to function
            df_result, excel_output, filename = process_fastag_data(file_data)

            # 3. Handle Errors
            if excel_output is None:
                return Response("Error processing Fastag PDF", status_code=400)

            # 4. Save to Disk
            generated_dir = DIRS["cleaner"] / "generated"
            os.makedirs(generated_dir, exist_ok=True)
            save_path = generated_dir / filename
            
            with open(save_path, "wb") as f:
                f.write(excel_output.read())

            # 5. Return Response
            row_count = len(df_result) if df_result is not None else 0
            return {
                "status": "success",
                "file_url": filename,
                "rows_processed": row_count,
                "db_rows_added": 0
            }

    except Exception as e:
        print(f"❌ Server Error: {e}")
        return Response(f"Internal Error: {e}", status_code=500)
        