from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.utils.geo import haversine_km
from app.utils.validators import is_blank, require_columns

BASE_DIR = Path(__file__).resolve().parents[2]
MASTER_FILE = BASE_DIR / "data" / "masters" / "aeropuertos_maestra.csv"


class IATAService:
    required_columns = ["IATA_origen", "IATA_destino"]

    def __init__(self) -> None:
        self.master_df = pd.read_csv(MASTER_FILE)
        self.master_df["iata_norm"] = self.master_df["iata"].astype(str).str.strip().str.upper()

    def _lookup_airport(self, iata_code: str) -> dict | None:
        iata_norm = str(iata_code).strip().upper()
        match = self.master_df[self.master_df["iata_norm"] == iata_norm]
        if match.empty:
            return None
        return match.iloc[0].to_dict()

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        require_columns(df, self.required_columns)
        results: list[dict] = []

        for _, row in df.iterrows():
            origin = row.get("IATA_origen")
            destination = row.get("IATA_destino")

            if is_blank(origin) or is_blank(destination):
                results.append({**row.to_dict(), "Estado": "FALTAN DATOS"})
                continue

            origin_norm = str(origin).strip().upper()
            destination_norm = str(destination).strip().upper()

            if len(origin_norm) != 3 or len(destination_norm) != 3:
                results.append(
                    {
                        **row.to_dict(),
                        "IATA_origen_norm": origin_norm,
                        "IATA_destino_norm": destination_norm,
                        "Estado": "FORMATO IATA INVÁLIDO",
                    }
                )
                continue

            origin_airport = self._lookup_airport(origin_norm)
            destination_airport = self._lookup_airport(destination_norm)

            if origin_airport is None or destination_airport is None:
                results.append(
                    {
                        **row.to_dict(),
                        "IATA_origen_norm": origin_norm,
                        "IATA_destino_norm": destination_norm,
                        "Lat_origen": None,
                        "Lon_origen": None,
                        "Aeropuerto_origen": None,
                        "Ciudad_origen": None,
                        "Pais_origen": None,
                        "Lat_destino": None,
                        "Lon_destino": None,
                        "Aeropuerto_destino": None,
                        "Ciudad_destino": None,
                        "Pais_destino": None,
                        "Distancia_km_aerea": None,
                        "Estado": "IATA NO ENCONTRADO",
                    }
                )
                continue

            distance_km = haversine_km(
                float(origin_airport["lat"]),
                float(origin_airport["lon"]),
                float(destination_airport["lat"]),
                float(destination_airport["lon"]),
            )

            results.append(
                {
                    **row.to_dict(),
                    "IATA_origen_norm": origin_norm,
                    "IATA_destino_norm": destination_norm,
                    "Lat_origen": origin_airport["lat"],
                    "Lon_origen": origin_airport["lon"],
                    "Aeropuerto_origen": origin_airport["airport_name"],
                    "Ciudad_origen": origin_airport["city"],
                    "Pais_origen": origin_airport["country"],
                    "Lat_destino": destination_airport["lat"],
                    "Lon_destino": destination_airport["lon"],
                    "Aeropuerto_destino": destination_airport["airport_name"],
                    "Ciudad_destino": destination_airport["city"],
                    "Pais_destino": destination_airport["country"],
                    "Distancia_km_aerea": round(distance_km, 2),
                    "Estado": "OK",
                }
            )

        return pd.DataFrame(results)
