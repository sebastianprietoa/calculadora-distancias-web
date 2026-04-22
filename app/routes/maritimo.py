from __future__ import annotations

import logging
import math
from pathlib import Path

from fastapi import APIRouter, File, HTTPException, Request, UploadFile
import numpy as np
import pandas as pd
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from app.services.maritimo_service import MaritimoService
from app.utils.excel import dataframe_to_excel_bytes, read_uploaded_table

logger = logging.getLogger(__name__)

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parents[1] / "templates"))
service = MaritimoService()


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


def _is_blankish(value: object) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except TypeError:
        pass
    return isinstance(value, str) and not value.strip()


def _coalesce_columns(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    result = pd.Series([None] * len(df), index=df.index, dtype=object)
    for column in columns:
        if column not in df.columns:
            continue
        values = df[column].astype(object)
        values = values.where(pd.notna(values), None)
        blank_mask = values.map(_is_blankish)
        values = values.mask(blank_mask, None)
        result_blank_mask = result.map(_is_blankish)
        result = result.where(~result_blank_mask, values)
    return result


def _join_observations(row: pd.Series) -> str | None:
    notes: list[str] = []
    for column in ["Observacion_origen", "Observacion_destino", "Observacion_lookup"]:
        value = row.get(column)
        if _is_blankish(value):
            continue
        text = str(value).strip()
        if text not in notes:
            notes.append(text)
    if not notes:
        return None
    return " | ".join(notes)


def _build_result_view_df(result_df: pd.DataFrame) -> pd.DataFrame:
    if result_df.empty:
        return pd.DataFrame(
            columns=[
                "Ciudad_origen",
                "Pais_origen",
                "Ciudad_destino",
                "Pais_destino",
                "Tipo_distancia",
                "Distancia_km",
                "Observacion",
                "Estado",
            ]
        )

    distance_km = pd.to_numeric(result_df.get("Distancia_km"), errors="coerce").round(2)

    view_df = pd.DataFrame(
        {
            "Ciudad_origen": _coalesce_columns(
                result_df,
                [
                    "Ciudad_origen_resuelta",
                    "Ciudad_origen",
                    "origin_port_name",
                    "Puerto_origen",
                    "Consulta_origen",
                    "Codigo_puerto_origen",
                    "origin_portcode",
                ],
            ),
            "Pais_origen": _coalesce_columns(
                result_df,
                ["Pais_origen_resuelto", "Pais_origen", "origin_country", "Country_origen"],
            ),
            "Ciudad_destino": _coalesce_columns(
                result_df,
                [
                    "Ciudad_destino_resuelta",
                    "Ciudad_destino",
                    "destination_port_name",
                    "Puerto_destino",
                    "Consulta_destino",
                    "Codigo_puerto_destino",
                    "destination_portcode",
                ],
            ),
            "Pais_destino": _coalesce_columns(
                result_df,
                ["Pais_destino_resuelto", "Pais_destino", "destination_country", "Country_destino"],
            ),
            "Tipo_distancia": _coalesce_columns(result_df, ["Tipo_distancia"]),
            "Distancia_km": distance_km.where(pd.notna(distance_km), None),
            "Observacion": result_df.apply(_join_observations, axis=1),
            "Estado": _coalesce_columns(result_df, ["Estado"]),
        }
    )
    return view_df


def _build_template_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "Codigo_puerto_origen",
            "Ciudad_origen",
            "Pais_origen",
            "Codigo_puerto_destino",
            "Ciudad_destino",
            "Pais_destino",
        ]
    )


@router.get("/maritimo", response_class=HTMLResponse)
def maritimo_page(request: Request):
    return templates.TemplateResponse("maritimo.html", {"request": request})


@router.get("/templates/maritimo")
def download_template():
    template_df = _build_template_df()
    excel_bytes = dataframe_to_excel_bytes(template_df, sheet_name="maritimo_template")
    return StreamingResponse(
        iter([excel_bytes]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="template_maritimo.xlsx"'},
    )


@router.post("/api/maritimo/preview")
async def preview_maritimo(file: UploadFile = File(...)):
    try:
        df = await read_uploaded_table(file)
        result_df = service.process(df)
        result_view_df = _build_result_view_df(result_df)

        total = int(len(result_df))
        ok_count = int((result_df["Estado"] == "OK").sum()) if total else 0
        status_series = result_df["Estado"].astype(str) if total else pd.Series(dtype=str)
        not_found_count = int(status_series.str.contains("NO ENCONTRADO", na=False).sum()) if total else 0
        invalid_count = int(status_series.str.contains("INVALIDO", na=False).sum()) if total else 0
        missing_count = int(status_series.str.contains(r"^FALTAN DATOS$|^FALTA ", regex=True, na=False).sum()) if total else 0
        proxy_count = int((result_df.get("Tipo_distancia") == "Distancia Proxy").sum()) if total else 0

        distance_km_series = _numeric_series(result_df, "Distancia_km")

        json_ready_df = _json_safe_df(result_view_df)
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
                    "proxy": proxy_count,
                    "distancia_total_km": _safe_json_float(distance_km_series.sum()),
                },
                "rows_preview": rows_preview,
                "missing_preview": missing_preview,
            }
        )
    except ValueError as exc:
        logger.warning("Error de validacion en vista previa maritima %s: %s", file.filename, exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Error interno en vista previa maritima %s", file.filename)
        raise HTTPException(status_code=500, detail="Error interno procesando vista previa") from exc


@router.post("/api/maritimo")
async def process_maritimo(file: UploadFile = File(...)):
    try:
        df = await read_uploaded_table(file)
        result_df = service.process(df)
        result_view_df = _build_result_view_df(result_df)
        excel_bytes = dataframe_to_excel_bytes(result_view_df, sheet_name="maritimo_output")
        return StreamingResponse(
            iter([excel_bytes]),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="maritimo_output.xlsx"'},
        )
    except ValueError as exc:
        logger.warning("Error de validacion procesando archivo maritimo %s: %s", file.filename, exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Error interno procesando archivo maritimo %s", file.filename)
        raise HTTPException(status_code=500, detail="Error interno procesando archivo") from exc
