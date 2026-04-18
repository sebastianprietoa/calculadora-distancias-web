from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, File, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, FileResponse, Response, StreamingResponse
from fastapi.templating import Jinja2Templates

from app.services.coordenadas_service import CoordenadasService
from app.utils.excel import dataframe_to_excel_bytes, read_uploaded_table

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
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error interno: {exc}") from exc
