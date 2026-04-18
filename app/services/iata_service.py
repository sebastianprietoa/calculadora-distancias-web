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

    def _extract_route_codes(self, row: pd.Series) -> list[str]:
        route_raw = row.get("Ruta_IATA")
        if not is_blank(route_raw):
            return [c.strip().upper() for c in str(route_raw).split("/") if c.strip()]

        origin = row.get("IATA_origen")
        destination = row.get("IATA_destino")

        if is_blank(origin) and is_blank(destination):
            return []

        if not is_blank(origin) and is_blank(destination) and "/" in str(origin):
            return [c.strip().upper() for c in str(origin).split("/") if c.strip()]

        if not is_blank(destination) and is_blank(origin) and "/" in str(destination):
            return [c.strip().upper() for c in str(destination).split("/") if c.strip()]

        origin_norm = str(origin).strip().upper() if not is_blank(origin) else ""
        destination_norm = str(destination).strip().upper() if not is_blank(destination) else ""
        return [c for c in [origin_norm, destination_norm] if c]

    def _validate_codes(self, codes: list[str]) -> bool:
        return all(len(code) == 3 for code in codes)

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        require_columns(df, self.required_columns)
        results: list[dict] = []

        for _, row in df.iterrows():
            route_codes = self._extract_route_codes(row)

            if len(route_codes) < 2:
                results.append({**row.to_dict(), "Estado": "FALTAN DATOS"})
                continue

            if not self._validate_codes(route_codes):
                results.append(
                    {
                        **row.to_dict(),
                        "Ruta_IATA_norm": "/".join(route_codes),
                        "Estado": "FORMATO IATA INVÁLIDO",
                    }
                )
                continue

            segment_distances: list[float] = []
            segment_details: list[str] = []
            missing_codes: set[str] = set()
            first_airport: dict | None = None
            last_airport: dict | None = None

            for idx in range(len(route_codes) - 1):
                code_a = route_codes[idx]
                code_b = route_codes[idx + 1]
                airport_a = self._lookup_airport(code_a)
                airport_b = self._lookup_airport(code_b)

                if airport_a is None:
                    missing_codes.add(code_a)
                if airport_b is None:
                    missing_codes.add(code_b)
                if airport_a is None or airport_b is None:
                    continue

                if first_airport is None:
                    first_airport = airport_a
                last_airport = airport_b

                distance_km = haversine_km(
                    float(airport_a["lat"]),
                    float(airport_a["lon"]),
                    float(airport_b["lat"]),
                    float(airport_b["lon"]),
                )
                segment_distances.append(distance_km)
                segment_details.append(f"{code_a}-{code_b}: {round(distance_km, 2)} km")

            if missing_codes:
                results.append(
                    {
                        **row.to_dict(),
                        "Ruta_IATA_norm": "/".join(route_codes),
                        "Tramos_calculados": len(segment_distances),
                        "Detalle_tramos": " | ".join(segment_details) if segment_details else None,
                        "Distancia_km": round(sum(segment_distances), 2) if segment_distances else None,
                        "Distancia_km_aerea": round(sum(segment_distances), 2) if segment_distances else None,
                        "Estado": f"IATA NO ENCONTRADO: {', '.join(sorted(missing_codes))}",
                    }
                )
                continue

            total_distance = round(sum(segment_distances), 2)
            results.append(
                {
                    **row.to_dict(),
                    "IATA_origen_norm": route_codes[0],
                    "IATA_destino_norm": route_codes[-1],
                    "Ruta_IATA_norm": "/".join(route_codes),
                    "Tramos_calculados": len(segment_distances),
                    "Detalle_tramos": " | ".join(segment_details),
                    "Lat_origen": first_airport["lat"] if first_airport else None,
                    "Lon_origen": first_airport["lon"] if first_airport else None,
                    "Aeropuerto_origen": first_airport["airport_name"] if first_airport else None,
                    "Ciudad_origen": first_airport["city"] if first_airport else None,
                    "Pais_origen": first_airport["country"] if first_airport else None,
                    "Lat_destino": last_airport["lat"] if last_airport else None,
                    "Lon_destino": last_airport["lon"] if last_airport else None,
                    "Aeropuerto_destino": last_airport["airport_name"] if last_airport else None,
                    "Ciudad_destino": last_airport["city"] if last_airport else None,
                    "Pais_destino": last_airport["country"] if last_airport else None,
                    "Distancia_km": total_distance,
                    "Distancia_km_aerea": total_distance,
                    "Estado": "OK",
                }
            )

        return pd.DataFrame(results)
