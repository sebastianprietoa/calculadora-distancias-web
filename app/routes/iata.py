from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from app.services.iata_service import IATAService
from app.utils.excel import dataframe_to_excel_bytes, read_uploaded_table

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parents[1] / "templates"))
service = IATAService()
BASE_DIR = Path(__file__).resolve().parents[2]


@router.get("/iata", response_class=HTMLResponse)
def iata_page(request: Request):
    return templates.TemplateResponse("iata.html", {"request": request})


@router.get("/templates/iata")
def download_template():
    file_path = BASE_DIR / "data" / "templates" / "template_iata.xlsx"
    return FileResponse(file_path, filename="template_iata.xlsx")


@router.post("/api/iata")
async def process_iata(file: UploadFile = File(...)):
    try:
        df = await read_uploaded_table(file)
        result_df = service.process(df)
        excel_bytes = dataframe_to_excel_bytes(result_df, sheet_name="aereo_iata_output")
        return StreamingResponse(
            iter([excel_bytes]),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="iata_output.xlsx"'},
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error interno: {exc}") from exc
