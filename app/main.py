from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.routes import coordenadas, iata, terrestre_ruta

BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "app" / "templates"
STATIC_DIR = BASE_DIR / "app" / "static"

app = FastAPI(title="Calculadora de Distancias Web", version="0.1.0")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

app.include_router(coordenadas.router)
app.include_router(iata.router)
app.include_router(terrestre_ruta.router)


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "tools": [
                {
                    "name": "Coordenadas",
                    "description": "Obtiene latitud y longitud a partir de columnas Ciudad y País.",
                    "path": "/coordenadas",
                },
                {
                    "name": "Distancias Aéreas",
                    "description": "Calcula distancia aérea entre códigos IATA origen y destino.",
                    "path": "/iata",
                },
                {
                    "name": "Terrestre por ruta",
                    "description": "Consulta distancia y duración por carretera usando OSRM.",
                    "path": "/terrestre-ruta",
                },
            ],
        },
    )


@app.get("/health")
def healthcheck():
    return {"status": "ok"}
