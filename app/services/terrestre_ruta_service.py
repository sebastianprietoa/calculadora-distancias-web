from __future__ import annotations

import pandas as pd
import requests

from app.utils.validators import require_columns


class TerrestreRutaService:
    required_columns = ["Latitud ori", "Longitud ori", "Latitud des", "Longitud des"]

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

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        require_columns(df, self.required_columns)
        results: list[dict] = []

        for _, row in df.iterrows():
            try:
                lat_ori = float(row["Latitud ori"])
                lon_ori = float(row["Longitud ori"])
                lat_des = float(row["Latitud des"])
                lon_des = float(row["Longitud des"])
            except Exception:
                results.append(
                    {
                        **row.to_dict(),
                        "Distancia_km_ruta": None,
                        "Duracion_min_ruta": None,
                        "Estado": "COORDENADAS INVÁLIDAS",
                    }
                )
                continue

            try:
                route = self._query_osrm(lat_ori, lon_ori, lat_des, lon_des)
                results.append(
                    {
                        **row.to_dict(),
                        "Distancia_km_ruta": round(route["distance"] / 1000, 2),
                        "Duracion_min_ruta": round(route["duration"] / 60, 2),
                        "Estado": "OK",
                    }
                )
            except Exception as exc:
                results.append(
                    {
                        **row.to_dict(),
                        "Distancia_km_ruta": None,
                        "Duracion_min_ruta": None,
                        "Estado": f"ERROR RUTA: {exc}",
                    }
                )

        return pd.DataFrame(results)
