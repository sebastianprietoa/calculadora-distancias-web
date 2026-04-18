from __future__ import annotations

import logging
import math
from pathlib import Path

from fastapi import APIRouter, File, HTTPException, Request, UploadFile
import numpy as np
import pandas as pd
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from app.services.terrestre_ruta_service import TerrestreRutaService
from app.utils.excel import dataframe_to_excel_bytes, read_uploaded_table

logger = logging.getLogger(__name__)

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parents[1] / "templates"))
service = TerrestreRutaService()
BASE_DIR = Path(__file__).resolve().parents[2]


def _json_safe_df(df: pd.DataFrame) -> pd.DataFrame:
    sanitized = df.replace([np.inf, -np.inf], np.nan).astype(object)
    return sanitized.where(pd.notna(sanitized), None)


def _safe_json_float(value: object, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(number):
        return default
    return round(number, 2)


@router.get("/terrestre-ruta", response_class=HTMLResponse)
def terrestre_page(request: Request):
    return templates.TemplateResponse("terrestre_ruta.html", {"request": request})


@router.get("/templates/terrestre-ruta")
def download_template():
    file_path = BASE_DIR / "data" / "templates" / "template_terrestre_ruta.xlsx"
    return FileResponse(file_path, filename="template_terrestre_ruta.xlsx")


@router.post("/api/terrestre-ruta/preview")
async def preview_terrestre_ruta(file: UploadFile = File(...)):
    try:
        df = await read_uploaded_table(file)
        result_df = service.process(df)

        total = int(len(result_df))
        ok_count = int((result_df["Estado"] == "OK").sum()) if total else 0
        invalid_count = int((result_df["Estado"] == "COORDENADAS INVÁLIDAS").sum()) if total else 0
        errors_count = int(result_df["Estado"].astype(str).str.startswith("ERROR").sum()) if total else 0

        distance_series = pd.to_numeric(result_df.get("Distancia_km_ruta", 0), errors="coerce")
        duration_series = pd.to_numeric(result_df.get("Duracion_min_ruta", 0), errors="coerce")
        distance_series = distance_series.replace([np.inf, -np.inf], np.nan).fillna(0)
        duration_series = duration_series.replace([np.inf, -np.inf], np.nan).fillna(0)

        json_ready_df = _json_safe_df(result_df)
        rows_preview = json_ready_df.head(200).to_dict(orient="records")
        missing_preview = json_ready_df[json_ready_df["Estado"] != "OK"].head(100).to_dict(orient="records")

        return JSONResponse(
            {
                "summary": {
                    "total": total,
                    "ok": ok_count,
                    "invalid": invalid_count,
                    "errors": errors_count,
                    "distancia_total_km": _safe_json_float(distance_series.sum()),
                    "duracion_total_min": _safe_json_float(duration_series.sum()),
                },
                "rows_preview": rows_preview,
                "missing_preview": missing_preview,
            }
        )
    except ValueError as exc:
        logger.warning("Error de validación en vista previa terrestre %s: %s", file.filename, exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Error interno en vista previa terrestre %s", file.filename)
        raise HTTPException(status_code=500, detail="Error interno procesando vista previa") from exc


@router.post("/api/terrestre-ruta")
async def process_terrestre_ruta(file: UploadFile = File(...)):
    try:
        df = await read_uploaded_table(file)
        result_df = service.process(df)
        excel_bytes = dataframe_to_excel_bytes(result_df, sheet_name="terrestre_ruta_output")
        return StreamingResponse(
            iter([excel_bytes]),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="terrestre_ruta_output.xlsx"'},
        )
    except ValueError as exc:
        logger.warning("Error de validación procesando %s: %s", file.filename, exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Error interno procesando %s", file.filename)
        raise HTTPException(status_code=500, detail="Error interno procesando archivo") from exc
