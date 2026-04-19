from __future__ import annotations

from io import BytesIO
from pathlib import Path

import pandas as pd
from fastapi import UploadFile


ALLOWED_SUFFIXES = {".xlsx", ".csv"}


def validate_extension(filename: str) -> str:
    suffix = Path(filename).suffix.lower()
    if suffix not in ALLOWED_SUFFIXES:
        raise ValueError("Formato no soportado. Usa .xlsx o .csv")
    return suffix


async def read_uploaded_table(upload: UploadFile) -> pd.DataFrame:
    suffix = validate_extension(upload.filename or "")
    content = await upload.read()
    if not content:
        raise ValueError("El archivo está vacío")

    try:
        if suffix == ".csv":
            return pd.read_csv(BytesIO(content))
        return pd.read_excel(BytesIO(content))
    except Exception as exc:
        raise ValueError(f"No se pudo leer el archivo {suffix}: {exc}") from exc


def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "resultado") -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    output.seek(0)
    return output.getvalue()
