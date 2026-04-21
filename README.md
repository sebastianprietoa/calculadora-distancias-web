# Calculadora de Distancias Web

Aplicación web en FastAPI para procesar archivos Excel/CSV con tres herramientas:

- Coordenadas (input por columnas `Ciudad` y `País`)
- Viajes IATA (`IATA_origen`, `IATA_destino`)
- Distancia terrestre por ruta (`Latitud ori`, `Longitud ori`, `Latitud des`, `Longitud des`)

## Requisitos

- Python 3.11+
- pip

## Instalación local

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# .venv\Scripts\activate   # Windows PowerShell
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Abre en tu navegador:

```text
http://127.0.0.1:8000
```

## Estructura

```text
app/
  main.py
  routes/
  services/
  utils/
  templates/
  static/
data/
  cache/
  masters/
  templates/
```

## Datos auxiliares

- `data/cache/geocache.csv`: caché local para coordenadas.
- `data/masters/aeropuertos_maestra.csv`: base maestra IATA principal.
- `data/masters/aeropuertos_supplemental.csv`: códigos IATA adicionales para cubrir rutas corporativas frecuentes.
- `data/templates/*.xlsx`: plantillas descargables desde la app.

## Notas

- La herramienta de coordenadas usa Nominatim/OpenStreetMap y primero busca en caché.
- La herramienta terrestre por ruta usa el servicio público de OSRM.
- La herramienta IATA usa una tabla maestra local de aeropuertos.

## Despliegue en Railway

Este repo incluye `Procfile`, `railway.json` y `Dockerfile`.
Solo debes conectar el repo en Railway y desplegar.


## UI Streamlit (opcional para pruebas internas)

Además de la UI FastAPI/Jinja, puedes abrir una UI avanzada para Coordenadas con Streamlit:

```bash
streamlit run streamlit_app.py
```

La UI Streamlit incluye:
- Progreso inicial (0%)
- Panel de alertas pre-descarga
- Resumen inicial "Esperando archivo"
- Validación previa antes de descargar el XLSX
