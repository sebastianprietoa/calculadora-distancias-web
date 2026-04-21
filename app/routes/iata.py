from __future__ import annotations

import logging
import math
from pathlib import Path

from fastapi import APIRouter, File, Form, HTTPException, Query, Request, UploadFile
import numpy as np
import pandas as pd
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from app.services.iata_service import IATAService
from app.utils.excel import dataframe_to_excel_bytes, read_uploaded_table

logger = logging.getLogger(__name__)

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parents[1] / "templates"))
service = IATAService()


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


def _numeric_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(dtype=float)
    series = pd.to_numeric(df[column], errors="coerce")
    return series.replace([np.inf, -np.inf], np.nan).fillna(0)


@router.get("/iata", response_class=HTMLResponse)
def iata_page(request: Request):
    return templates.TemplateResponse("iata.html", {"request": request})


@router.get("/templates/iata")
def download_template(mode: str = Query(default="corporativo")):
    mode_norm = (mode or "corporativo").strip().lower()
    if mode_norm == "upstream":
        template_df = pd.DataFrame(columns=["IATA_origen", "Ciudad_origen", "Pais_origen"])
        filename = "template_iata_upstream.xlsx"
    elif mode_norm == "downstream":
        template_df = pd.DataFrame(columns=["IATA_destino", "Ciudad_destino", "Pais_destino"])
        filename = "template_iata_downstream.xlsx"
    else:
        template_df = pd.DataFrame(columns=["Origen", "Destino", "Ruta"])
        filename = "template_iata_corporativo.xlsx"
    excel_bytes = dataframe_to_excel_bytes(template_df, sheet_name="iata_template")
    return StreamingResponse(
        iter([excel_bytes]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/api/iata/preview")
async def preview_iata(
    file: UploadFile = File(...),
    composite_mode: str = Form(""),
    plant_address: str = Form(""),
    plant_city: str = Form(""),
    plant_country: str = Form(""),
    plant_airport_iata: str = Form(""),
):
    try:
        df = await read_uploaded_table(file)
        result_df = service.process(
            df,
            composite_mode=composite_mode,
            plant_address=plant_address,
            plant_city=plant_city,
            plant_country=plant_country,
            plant_airport_iata=plant_airport_iata,
        )

        total = int(len(result_df))
        ok_count = int((result_df["Estado"] == "OK").sum()) if total else 0
        not_found_count = int(result_df["Estado"].astype(str).str.startswith("IATA").sum()) if total else 0
        invalid_count = int((result_df["Estado"] == "FORMATO IATA INVÁLIDO").sum()) if total else 0
        missing_count = int((result_df["Estado"] == "FALTAN DATOS").sum()) if total else 0

        distance_series = _numeric_series(result_df, "Distancia_km")
        total_distance = _safe_json_float(distance_series.sum())

        composed_series = _numeric_series(result_df, "Distancia_total_compuesta_km")
        composed_total = _safe_json_float(composed_series.sum())

        json_ready_df = _json_safe_df(result_df)
        rows_preview = json_ready_df.head(200).to_dict(orient="records")
        missing_preview = json_ready_df[json_ready_df["Estado"] != "OK"].head(100).to_dict(orient="records")

        return JSONResponse(
            {
                "summary": {
                    "total": total,
                    "ok": ok_count,
                    "not_found": not_found_count,
                    "invalid": invalid_count,
                    "missing": missing_count,
                    "distancia_total_km": total_distance,
                    "distancia_compuesta_total_km": composed_total,
                },
                "rows_preview": rows_preview,
                "missing_preview": missing_preview,
            }
        )
    except ValueError as exc:
        logger.warning("Error de validación en vista previa IATA %s: %s", file.filename, exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Error interno en vista previa IATA %s", file.filename)
        raise HTTPException(status_code=500, detail="Error interno procesando vista previa") from exc


@router.post("/api/iata")
async def process_iata(
    file: UploadFile = File(...),
    composite_mode: str = Form(""),
    plant_address: str = Form(""),
    plant_city: str = Form(""),
    plant_country: str = Form(""),
    plant_airport_iata: str = Form(""),
):
    try:
        df = await read_uploaded_table(file)
        result_df = service.process(
            df,
            composite_mode=composite_mode,
            plant_address=plant_address,
            plant_city=plant_city,
            plant_country=plant_country,
            plant_airport_iata=plant_airport_iata,
        )
        excel_bytes = dataframe_to_excel_bytes(result_df, sheet_name="aereo_iata_output")
        return StreamingResponse(
            iter([excel_bytes]),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="iata_output.xlsx"'},
        )
    except ValueError as exc:
        logger.warning("Error de validación procesando %s: %s", file.filename, exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Error interno procesando %s", file.filename)
        raise HTTPException(status_code=500, detail="Error interno procesando archivo") from exc
