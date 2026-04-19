from __future__ import annotations

from time import sleep

import pandas as pd
import requests

from app.utils.validators import is_blank, parse_float_in_range


class TerrestreRutaService:
    required_coord_columns = ["Latitud ori", "Longitud ori", "Latitud des", "Longitud des"]

    def _query_osrm(self, lat_ori: float, lon_ori: float, lat_des: float, lon_des: float) -> dict:
        url = (
            "https://router.project-osrm.org/route/v1/driving/"
            f"{lon_ori},{lat_ori};{lon_des},{lat_des}"
        )
        response = requests.get(url, params={"overview": "false"}, timeout=30)
        response.raise_for_status()
        data = response.json()
        routes = data.get("routes", [])
        if not routes:
            raise ValueError("Sin rutas encontradas")
        return routes[0]

    def _geocode(self, query: str) -> tuple[float, float] | None:
        response = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": query, "format": "jsonv2", "limit": 1},
            headers={"User-Agent": "calculadora-distancias-web/0.1 (internal testing)"},
            timeout=20,
        )
        response.raise_for_status()
        data = response.json()
        sleep(1)
        if not data:
            return None
        return float(data[0]["lat"]), float(data[0]["lon"])

    def _pick(self, row: pd.Series, options: list[str]) -> object:
        for col in options:
            if col in row.index:
                return row.get(col)
        return None

    def _resolve_point_from_text(self, row: pd.Series, suffix: str) -> tuple[float, float, str, str | None]:
        direccion = self._pick(row, [f"Direccion {suffix}", f"Dirección {suffix}", f"Direccion_{suffix}", f"Dirección_{suffix}"])
        ciudad = self._pick(row, [f"Ciudad {suffix}", f"Ciudad_{suffix}"])
        pais = self._pick(row, [f"Pais {suffix}", f"País {suffix}", f"Pais_{suffix}", f"País_{suffix}"])

        if is_blank(pais):
            raise ValueError(f"FALTA PAÍS {suffix.upper()}")

        pais_str = str(pais).strip()
        used_fallback = None

        if is_blank(ciudad):
            ciudad_str = f"capital de {pais_str}"
            used_fallback = f"Se usó capital de {pais_str}"
        else:
            ciudad_str = str(ciudad).strip()

        query_parts: list[str] = []
        if not is_blank(direccion):
            query_parts.append(str(direccion).strip())
        query_parts.extend([ciudad_str, pais_str])
        query = ", ".join([part for part in query_parts if part])

        point = self._geocode(query)
        if point is None:
            raise ValueError(f"NO ENCONTRADO {suffix.upper()}")

        return point[0], point[1], query, used_fallback

    def _resolve_coords_from_row(self, row: pd.Series) -> tuple[float, float, float, float]:
        lat_ori = parse_float_in_range(row["Latitud ori"], -90, 90)
        lon_ori = parse_float_in_range(row["Longitud ori"], -180, 180)
        lat_des = parse_float_in_range(row["Latitud des"], -90, 90)
        lon_des = parse_float_in_range(row["Longitud des"], -180, 180)
        return lat_ori, lon_ori, lat_des, lon_des

    def _row_has_valid_coords(self, row: pd.Series) -> bool:
        try:
            self._resolve_coords_from_row(row)
            return True
        except Exception:
            return False

    def process(self, df: pd.DataFrame, mode: str = "auto") -> pd.DataFrame:
        mode_norm = (mode or "auto").strip().lower()
        if mode_norm not in {"auto", "coordenadas", "direccion"}:
            raise ValueError("Modo inválido. Usa auto, coordenadas o direccion")

        results: list[dict] = []

        for _, row in df.iterrows():
            lat_ori = lon_ori = lat_des = lon_des = None
            consulta_ori = consulta_des = None
            fallback_ori = fallback_des = None
            modo_fila = mode_norm

            try:
                if mode_norm == "coordenadas":
                    lat_ori, lon_ori, lat_des, lon_des = self._resolve_coords_from_row(row)
                elif mode_norm == "direccion":
                    lat_ori, lon_ori, consulta_ori, fallback_ori = self._resolve_point_from_text(row, "ori")
                    lat_des, lon_des, consulta_des, fallback_des = self._resolve_point_from_text(row, "des")
                else:  # auto
                    if self._row_has_valid_coords(row):
                        lat_ori, lon_ori, lat_des, lon_des = self._resolve_coords_from_row(row)
                        modo_fila = "coordenadas"
                    else:
                        lat_ori, lon_ori, consulta_ori, fallback_ori = self._resolve_point_from_text(row, "ori")
                        lat_des, lon_des, consulta_des, fallback_des = self._resolve_point_from_text(row, "des")
                        modo_fila = "direccion"
            except Exception as exc:
                results.append(
                    {
                        **row.to_dict(),
                        "Modo_entrada": modo_fila,
                        "Consulta_ori": consulta_ori,
                        "Consulta_des": consulta_des,
                        "Fallback_ori": fallback_ori,
                        "Fallback_des": fallback_des,
                        "Latitud_ori_usada": lat_ori,
                        "Longitud_ori_usada": lon_ori,
                        "Latitud_des_usada": lat_des,
                        "Longitud_des_usada": lon_des,
                        "Distancia_km_ruta": None,
                        "Duracion_min_ruta": None,
                        "Estado": f"ENTRADA INVÁLIDA: {exc}",
                    }
                )
                continue

            try:
                route = self._query_osrm(lat_ori, lon_ori, lat_des, lon_des)
                results.append(
                    {
                        **row.to_dict(),
                        "Modo_entrada": modo_fila,
                        "Consulta_ori": consulta_ori,
                        "Consulta_des": consulta_des,
                        "Fallback_ori": fallback_ori,
                        "Fallback_des": fallback_des,
                        "Latitud_ori_usada": lat_ori,
                        "Longitud_ori_usada": lon_ori,
                        "Latitud_des_usada": lat_des,
                        "Longitud_des_usada": lon_des,
                        "Distancia_km_ruta": round(route["distance"] / 1000, 2),
                        "Duracion_min_ruta": round(route["duration"] / 60, 2),
                        "Estado": "OK",
                    }
                )
            except Exception as exc:
                results.append(
                    {
                        **row.to_dict(),
                        "Modo_entrada": modo_fila,
                        "Consulta_ori": consulta_ori,
                        "Consulta_des": consulta_des,
                        "Fallback_ori": fallback_ori,
                        "Fallback_des": fallback_des,
                        "Latitud_ori_usada": lat_ori,
                        "Longitud_ori_usada": lon_ori,
                        "Latitud_des_usada": lat_des,
                        "Longitud_des_usada": lon_des,
                        "Distancia_km_ruta": None,
                        "Duracion_min_ruta": None,
                        "Estado": f"ERROR RUTA: {exc}",
                    }
                )

        return pd.DataFrame(results)
