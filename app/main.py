from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.routes import coordenadas, iata, maritimo, terrestre_ruta

BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "app" / "templates"
STATIC_DIR = BASE_DIR / "app" / "static"

app = FastAPI(title="Calculadora de Distancias Web", version="0.1.0")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

app.include_router(coordenadas.router)
app.include_router(iata.router)
app.include_router(maritimo.router)
app.include_router(terrestre_ruta.router)


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "overview_stats": [
                {
                    "label": "Modulos",
                    "value": "4 operativos",
                    "icon_path": "/static/icons/grid.svg",
                },
                {
                    "label": "Plantillas",
                    "value": "4 listas",
                    "icon_path": "/static/icons/sheet.svg",
                },
                {
                    "label": "Revision",
                    "value": "Vista previa",
                    "icon_path": "/static/icons/review.svg",
                },
                {
                    "label": "Exportacion",
                    "value": "XLSX final",
                    "icon_path": "/static/icons/download.svg",
                },
            ],
            "tools": [
                {
                    "name": "Coordenadas",
                    "description": "Obtiene latitud y longitud estandarizadas a partir de columnas Ciudad y Pais.",
                    "path": "/coordenadas",
                    "template_path": "/templates/coordenadas",
                    "theme": "green",
                    "icon_path": "/static/icons/coordenadas.svg",
                    "meta": "Geocodificacion y validacion previa",
                    "summary_items": [
                        {
                            "label": "Entrada",
                            "value": "Ciudad + Pais",
                            "icon_path": "/static/icons/sheet.svg",
                        },
                        {
                            "label": "Revision",
                            "value": "Vista previa",
                            "icon_path": "/static/icons/review.svg",
                        },
                        {
                            "label": "Salida",
                            "value": "XLSX final",
                            "icon_path": "/static/icons/download.svg",
                        },
                    ],
                },
                {
                    "name": "Distancias Aereas",
                    "description": "Calcula distancias aereas para viajes corporativos y flujos upstream o downstream.",
                    "path": "/iata",
                    "template_path": "/templates/iata?mode=corporativo",
                    "theme": "teal",
                    "icon_path": "/static/icons/iata.svg",
                    "meta": "3 modos de calculo y soporte IATA",
                    "summary_items": [
                        {
                            "label": "Modos",
                            "value": "3 flujos",
                            "icon_path": "/static/icons/grid.svg",
                        },
                        {
                            "label": "Entrada",
                            "value": "IATA o ruta",
                            "icon_path": "/static/icons/sheet.svg",
                        },
                        {
                            "label": "Salida",
                            "value": "XLSX final",
                            "icon_path": "/static/icons/download.svg",
                        },
                    ],
                },
                {
                    "name": "Terrestre por ruta",
                    "description": "Consulta distancia y duracion por carretera usando coordenadas o texto estructurado.",
                    "path": "/terrestre-ruta",
                    "template_path": "/templates/terrestre-ruta?mode=auto",
                    "theme": "amber",
                    "icon_path": "/static/icons/terrestre.svg",
                    "meta": "Auto, coordenadas o direccion",
                    "summary_items": [
                        {
                            "label": "Modo",
                            "value": "Auto o manual",
                            "icon_path": "/static/icons/grid.svg",
                        },
                        {
                            "label": "Resultado",
                            "value": "Km + duracion",
                            "icon_path": "/static/icons/review.svg",
                        },
                        {
                            "label": "Salida",
                            "value": "XLSX final",
                            "icon_path": "/static/icons/download.svg",
                        },
                    ],
                },
                {
                    "name": "Distancias Maritimas",
                    "description": "Resuelve distancias maritimas por codigo de puerto o por ciudad y pais.",
                    "path": "/maritimo",
                    "template_path": "/templates/maritimo",
                    "theme": "blue",
                    "icon_path": "/static/icons/maritimo.svg",
                    "meta": "Exactas y proxy por mismo pais",
                    "summary_items": [
                        {
                            "label": "Entrada",
                            "value": "Puerto o ciudad",
                            "icon_path": "/static/icons/sheet.svg",
                        },
                        {
                            "label": "Fallback",
                            "value": "Proxy por pais",
                            "icon_path": "/static/icons/review.svg",
                        },
                        {
                            "label": "Salida",
                            "value": "Km y observacion",
                            "icon_path": "/static/icons/download.svg",
                        },
                    ],
                },
            ],
        },
    )


@app.get("/health")
def healthcheck():
    return {"status": "ok"}
