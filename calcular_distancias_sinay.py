import os
import json
import time
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd
import requests

# =========================
# CONFIGURACIÓN GENERAL
# =========================
API_KEY = os.getenv("SINAY_API_KEY", "")
BASE_URL = "https://api.sinay.ai/co2/api/v2/compute-co2"
DEFAULT_IMO = 9706906  # Reemplaza por el IMO que quieras usar como referencia

# Carpeta principal sugerida por el usuario
PROJECT_DIR = Path(r"D:\GREEN_TICKET\proyectos\calculadora-distancias-web\calculadora-distancias-web")
DEFAULT_INPUT_FILENAME = "input_coronel_espana.xlsx"
DEFAULT_OUTPUT_FILENAME = "distancias_coronel_espana.xlsx"
CACHE_FILENAME = "distance_cache_coronel_espana.json"

# Hoja de entrada esperada en el Excel
INPUT_SHEET = "Input_Routes"

# Ajustes de requests
REQUEST_DELAY_SECONDS = 0.35
MAX_RETRIES = 3
TIMEOUT = 45


# =========================
# UTILIDADES DE ARCHIVOS
# =========================
def find_input_file() -> Path:
    """
    Busca el archivo de input en este orden:
    1) Variable de entorno INPUT_FILE
    2) Ruta exacta del proyecto + nombre por defecto
    3) Carpeta donde está este script
    4) Solicita la ruta por consola
    """
    env_input = os.getenv("INPUT_FILE")
    if env_input:
        env_path = Path(env_input)
        if env_path.exists():
            return env_path
        raise FileNotFoundError(f"La ruta indicada en INPUT_FILE no existe: {env_input}")

    candidate_1 = PROJECT_DIR / DEFAULT_INPUT_FILENAME
    if candidate_1.exists():
        return candidate_1

    script_dir = Path(__file__).resolve().parent
    candidate_2 = script_dir / DEFAULT_INPUT_FILENAME
    if candidate_2.exists():
        return candidate_2

    user_path = input(
        "No encontré el archivo de input automáticamente. \n"
        "Pega la ruta completa del Excel de entrada y presiona Enter:\n> "
    ).strip().strip('"')

    custom_path = Path(user_path)
    if not custom_path.exists():
        raise FileNotFoundError(f"No existe el archivo indicado: {custom_path}")

    return custom_path


def build_output_path(input_path: Path) -> Path:
    preferred_output = PROJECT_DIR / DEFAULT_OUTPUT_FILENAME
    if PROJECT_DIR.exists():
        return preferred_output
    return input_path.with_name(DEFAULT_OUTPUT_FILENAME)


def build_cache_path(input_path: Path) -> Path:
    preferred_cache = PROJECT_DIR / CACHE_FILENAME
    if PROJECT_DIR.exists():
        return preferred_cache
    return input_path.with_name(CACHE_FILENAME)


def load_cache(cache_path: Path) -> Dict[str, Any]:
    if cache_path.exists():
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache: Dict[str, Any], cache_path: Path) -> None:
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


# =========================
# API SINAY
# =========================
def build_cache_key(origin: str, destination: str, imo: int) -> str:
    return f"{origin.strip().upper()}__{destination.strip().upper()}__IMO_{imo}"


def get_maritime_distance_nm(origin_portcode: str, destination_portcode: str, imo: int = DEFAULT_IMO) -> Dict[str, Any]:
    headers = {
        "Content-Type": "application/json",
        "API_KEY": API_KEY,
    }

    payload = {
        "vessel": {"imo": imo},
        "departure": {"portCode": origin_portcode.strip().upper()},
        "arrival": {"portCode": destination_portcode.strip().upper()},
    }

    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(BASE_URL, headers=headers, json=payload, timeout=TIMEOUT)

            if response.status_code == 200:
                data = response.json()
                return {
                    "success": True,
                    "length_nm": data.get("length"),
                    "raw_response": data,
                    "error": None,
                }

            last_error = f"HTTP {response.status_code}: {response.text}"

        except requests.RequestException as e:
            last_error = str(e)

        if attempt < MAX_RETRIES:
            time.sleep(attempt * 1.5)

    return {
        "success": False,
        "length_nm": None,
        "raw_response": None,
        "error": last_error,
    }


# =========================
# LECTURA / ESCRITURA EXCEL
# =========================
def read_input_routes(input_path: Path) -> pd.DataFrame:
    df = pd.read_excel(input_path, sheet_name=INPUT_SHEET)

    required_cols = {
        "route_id",
        "origin_portcode",
        "origin_port_name",
        "destination_portcode",
        "destination_port_name",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            f"La hoja '{INPUT_SHEET}' no contiene todas las columnas requeridas. Faltan: {sorted(missing)}"
        )

    return df


def write_output_excel(results_df: pd.DataFrame, original_df: pd.DataFrame, output_path: Path) -> None:
    summary = pd.DataFrame(
        {
            "metric": [
                "total_rutas",
                "rutas_ok",
                "rutas_error",
                "distancia_min_nm",
                "distancia_max_nm",
                "distancia_promedio_nm",
            ],
            "value": [
                len(results_df),
                int((results_df["status"] == "ok").sum()),
                int((results_df["status"] == "error").sum()),
                results_df["distance_nm"].min(skipna=True),
                results_df["distance_nm"].max(skipna=True),
                results_df["distance_nm"].mean(skipna=True),
            ],
        }
    )

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        results_df.to_excel(writer, sheet_name="Resultados", index=False)
        summary.to_excel(writer, sheet_name="Resumen", index=False)
        original_df.to_excel(writer, sheet_name="Input_Usado", index=False)


# =========================
# PROCESAMIENTO PRINCIPAL
# =========================
def process_routes(df: pd.DataFrame, cache_path: Path, imo: int = DEFAULT_IMO) -> pd.DataFrame:
    cache = load_cache(cache_path)
    results = []
    total = len(df)

    for i, row in df.iterrows():
        origin = str(row["origin_portcode"]).strip().upper()
        destination = str(row["destination_portcode"]).strip().upper()
        cache_key = build_cache_key(origin, destination, imo)

        if cache_key in cache:
            cached = cache[cache_key]
            results.append(
                {
                    **row.to_dict(),
                    "distance_nm": cached.get("distance_nm"),
                    "status": cached.get("status"),
                    "error": cached.get("error"),
                    "source": "cache",
                }
            )
            continue

        result = get_maritime_distance_nm(origin, destination, imo=imo)

        item = {
            **row.to_dict(),
            "distance_nm": result["length_nm"],
            "status": "ok" if result["success"] else "error",
            "error": result["error"],
            "source": "api",
        }
        results.append(item)

        cache[cache_key] = {
            "distance_nm": item["distance_nm"],
            "status": item["status"],
            "error": item["error"],
        }

        if (i + 1) % 20 == 0 or (i + 1) == total:
            print(f"Procesadas {i + 1}/{total} rutas...")
            save_cache(cache, cache_path)

        time.sleep(REQUEST_DELAY_SECONDS)

    save_cache(cache, cache_path)
    return pd.DataFrame(results)


def main() -> None:
    if not API_KEY:
        raise ValueError(
            "No se encontró la variable de entorno SINAY_API_KEY. "
            "Debes configurarla antes de ejecutar el script."
        )

    input_path = find_input_file()
    output_path = build_output_path(input_path)
    cache_path = build_cache_path(input_path)

    print(f"Archivo input encontrado: {input_path}")
    print(f"Archivo output: {output_path}")
    print(f"Archivo cache: {cache_path}")

    df = read_input_routes(input_path)
    results_df = process_routes(df, cache_path=cache_path, imo=DEFAULT_IMO)
    write_output_excel(results_df, df, output_path)

    ok_count = int((results_df["status"] == "ok").sum())
    err_count = int((results_df["status"] == "error").sum())

    print("\nProceso terminado.")
    print(f"Rutas exitosas: {ok_count}")
    print(f"Rutas con error: {err_count}")
    print(f"Resultado guardado en: {output_path}")


if __name__ == "__main__":
    main()
