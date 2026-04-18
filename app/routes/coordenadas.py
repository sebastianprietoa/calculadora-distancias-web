from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter, File, HTTPException, Request, UploadFile
import pandas as pd
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from app.services.coordenadas_service import CoordenadasService
from app.utils.excel import dataframe_to_excel_bytes, read_uploaded_table

logger = logging.getLogger(__name__)

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parents[1] / "templates"))
service = CoordenadasService()
BASE_DIR = Path(__file__).resolve().parents[2]


@router.get("/coordenadas", response_class=HTMLResponse)
def coordenadas_page(request: Request):
    return templates.TemplateResponse("coordenadas.html", {"request": request})


@router.get("/templates/coordenadas")
def download_template():
    file_path = BASE_DIR / "data" / "templates" / "template_coordenadas.xlsx"
    return FileResponse(file_path, filename="template_coordenadas.xlsx")




@router.post("/api/coordenadas/preview")
async def preview_coordenadas(file: UploadFile = File(...)):
    try:
        df = await read_uploaded_table(file)
        result_df = service.process(df)

        total = int(len(result_df))
        ok_count = int((result_df["Estado"] == "OK").sum()) if total else 0
        not_found_count = int((result_df["Estado"] == "NO ENCONTRADO").sum()) if total else 0
        errors_count = int((result_df["Estado"].str.startswith("ERROR", na=False)).sum()) if total else 0
        missing_count = int((result_df["Estado"] == "FALTAN DATOS").sum()) if total else 0

        mean_precision = round(float(result_df.get("Precision_pct", 0).fillna(0).mean()), 2) if total else 0.0

        json_ready_df = result_df.where(pd.notna(result_df), None)
        rows_preview = json_ready_df.head(200).to_dict(orient="records")
        missing_preview = json_ready_df[json_ready_df["Estado"] != "OK"].head(100).to_dict(orient="records")

        return JSONResponse(
            {
                "summary": {
                    "total": total,
                    "ok": ok_count,
                    "not_found": not_found_count,
                    "errors": errors_count,
                    "missing": missing_count,
                    "precision_pct": mean_precision,
                },
                "rows_preview": rows_preview,
                "missing_preview": missing_preview,
                "preview_limit": 200,
                "missing_limit": 100,
            }
        )
    except ValueError as exc:
        logger.warning("Error de validación en vista previa %s: %s", file.filename, exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Error interno en vista previa %s", file.filename)
        raise HTTPException(status_code=500, detail="Error interno procesando vista previa") from exc

@router.post("/api/coordenadas")
async def process_coordenadas(file: UploadFile = File(...)):
    try:
        df = await read_uploaded_table(file)
        result_df = service.process(df)
        excel_bytes = dataframe_to_excel_bytes(result_df, sheet_name="coordenadas_output")
        return StreamingResponse(
            iter([excel_bytes]),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="coordenadas_output.xlsx"'},
        )
    except ValueError as exc:
        logger.warning("Error de validación procesando %s: %s", file.filename, exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Error interno procesando %s", file.filename)
        raise HTTPException(status_code=500, detail="Error interno procesando archivo") from exc
