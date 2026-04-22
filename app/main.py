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
            "tools": [
                {
                    "name": "Coordenadas",
                    "description": "Obtiene latitud y longitud estandarizadas a partir de columnas Ciudad y Pais.",
                    "path": "/coordenadas",
                    "template_path": "/templates/coordenadas",
                    "theme": "green",
                    "meta": "Geocodificacion y validacion previa",
                    "highlights": [
                        "Carga planillas CSV o XLSX con columnas Ciudad y Pais.",
                        "Valida registros antes de habilitar la descarga final.",
                        "Entrega vista previa con precision y observaciones.",
                    ],
                },
                {
                    "name": "Distancias Aereas",
                    "description": "Calcula distancias aereas para viajes corporativos y flujos upstream o downstream.",
                    "path": "/iata",
                    "template_path": "/templates/iata?mode=corporativo",
                    "theme": "teal",
                    "meta": "3 modos de calculo y soporte IATA",
                    "highlights": [
                        "Segmenta entre viaje corporativo, upstream y downstream.",
                        "Acepta rutas IATA y complementa distancia compuesta cuando aplica.",
                        "Mantiene una misma experiencia de validacion y descarga.",
                    ],
                },
                {
                    "name": "Terrestre por ruta",
                    "description": "Consulta distancia y duracion por carretera usando coordenadas o texto estructurado.",
                    "path": "/terrestre-ruta",
                    "template_path": "/templates/terrestre-ruta?mode=auto",
                    "theme": "amber",
                    "meta": "Auto, coordenadas o direccion",
                    "highlights": [
                        "Opera con coordenadas o direccion, ciudad y pais.",
                        "Entrega distancia total y duracion agregada del lote.",
                        "Resume errores de formato antes de procesar el archivo final.",
                    ],
                },
                {
                    "name": "Distancias Maritimas",
                    "description": "Resuelve distancias maritimas por codigo de puerto o por ciudad y pais.",
                    "path": "/maritimo",
                    "template_path": "/templates/maritimo",
                    "theme": "blue",
                    "meta": "Exactas y proxy por mismo pais",
                    "highlights": [
                        "Tolera variantes de escritura y alias de paises.",
                        "Prioriza distancia exacta y cae a Distancia Proxy si falta la ciudad.",
                        "Exporta un XLSX limpio centrado en ciudad, pais y km.",
                    ],
                },
            ],
        },
    )


@app.get("/health")
def healthcheck():
    return {"status": "ok"}
