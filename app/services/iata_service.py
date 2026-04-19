from __future__ import annotations

from pathlib import Path
from time import sleep
import unicodedata

import pandas as pd
import requests

from app.utils.geo import haversine_km
from app.utils.validators import is_blank

BASE_DIR = Path(__file__).resolve().parents[2]
MASTER_FILE = BASE_DIR / "data" / "masters" / "aeropuertos_maestra.csv"


class IATAService:
    required_columns = ["IATA_origen", "IATA_destino"]
    iata_aliases = {
        "PMC": "PMC",  # Aeropuerto El Tepual (Puerto Montt)
    }
    country_aliases = {
        "espana": "spain",
        "españa": "spain",
        "eeuu": "united states",
        "estados unidos": "united states",
        "brasil": "brazil",
        "peru": "peru",
        "chile": "chile",
        "argentina": "argentina",
    }

    def __init__(self) -> None:
        self.master_df = pd.read_csv(MASTER_FILE)
        self.master_df["iata_norm"] = self.master_df["iata"].astype(str).str.strip().str.upper()
        self._inject_missing_airports()

    def _inject_missing_airports(self) -> None:
        manual_airports = [
            {
                "iata": "PMC",
                "airport_name": "El Tepual Airport",
                "city": "Puerto Montt",
                "country": "Chile",
                "lat": -41.4389,
                "lon": -73.0939,
                "iata_norm": "PMC",
            },
            {
                "iata": "EZE",
                "airport_name": "Ministro Pistarini International Airport",
                "city": "Buenos Aires",
                "country": "Argentina",
                "lat": -34.8222,
                "lon": -58.5358,
                "iata_norm": "EZE",
            }
        ]
        for airport in manual_airports:
            exists = not self.master_df[self.master_df["iata_norm"] == airport["iata_norm"]].empty
            if not exists:
                self.master_df = pd.concat([self.master_df, pd.DataFrame([airport])], ignore_index=True)

    def _normalize_text(self, value: str | None) -> str:
        if is_blank(value):
            return ""
        text = str(value).strip().lower()
        text = unicodedata.normalize("NFKD", text)
        return "".join(ch for ch in text if not unicodedata.combining(ch))

    def _lookup_airport(self, iata_code: str | None) -> dict | None:
        if is_blank(iata_code):
            return None
        iata_norm = str(iata_code).strip().upper()
        iata_norm = self.iata_aliases.get(iata_norm, iata_norm)
        match = self.master_df[self.master_df["iata_norm"] == iata_norm]
        if match.empty:
            return None
        return match.iloc[0].to_dict()

    def _lookup_airport_by_city_country(self, city: str | None, country: str | None) -> dict | None:
        city_norm = self._normalize_text(city)
        country_norm = self._normalize_country(country)
        if not city_norm or not country_norm:
            return None

        country_df = self.master_df[
            self.master_df["country"].astype(str).map(self._normalize_country) == country_norm
        ]
        if country_df.empty:
            return None

        exact = country_df[country_df["city"].astype(str).map(self._normalize_text) == city_norm]
        if not exact.empty:
            return exact.sort_values(by=["airport_name"]).iloc[0].to_dict()

        partial = country_df[country_df["city"].astype(str).map(self._normalize_text).str.contains(city_norm, na=False)]
        if partial.empty:
            partial = country_df[country_df["city"].astype(str).map(self._normalize_text).apply(lambda c: city_norm in c)]
        if partial.empty:
            return None
        return partial.sort_values(by=["airport_name"]).iloc[0].to_dict()

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

    def _row_value(self, row: pd.Series, candidates: list[str]) -> str | None:
        for name in candidates:
            if name in row and not is_blank(row[name]):
                return str(row[name]).strip()
        return None

    def _resolve_row_airport(self, row: pd.Series, kind: str) -> dict | None:
        if kind == "origin":
            iata = self._row_value(row, ["IATA_origen", "iata_origen", "Codigo_IATA_origen", "Codigo IATA origen"])
            if not is_blank(iata):
                return self._lookup_airport(iata)
            city = self._row_value(row, ["Ciudad_origen", "Ciudad origen", "city_origen"])
            country = self._row_value(row, ["Pais_origen", "País_origen", "Pais origen", "País origen", "country_origen"])
            return self._lookup_airport_by_city_country(city, country)

        iata = self._row_value(row, ["IATA_destino", "iata_destino", "Codigo_IATA_destino", "Codigo IATA destino"])
        if not is_blank(iata):
            return self._lookup_airport(iata)
        city = self._row_value(row, ["Ciudad_destino", "Ciudad destino", "city_destino"])
        country = self._row_value(row, ["Pais_destino", "País_destino", "Pais destino", "País destino", "country_destino"])
        return self._lookup_airport_by_city_country(city, country)

    def _geocode_plant(self, address: str | None, city: str | None, country: str | None) -> tuple[float, float] | None:
        if is_blank(country):
            return None

        pieces: list[str] = []
        if not is_blank(address):
            pieces.append(str(address).strip())
        if not is_blank(city):
            pieces.append(str(city).strip())
        else:
            pieces.append(f"capital de {str(country).strip()}")
        pieces.append(str(country).strip())

        response = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": ", ".join(pieces), "format": "jsonv2", "limit": 1},
            headers={"User-Agent": "calculadora-distancias-web/0.1 (internal testing)"},
            timeout=20,
        )
        response.raise_for_status()
        data = response.json()
        sleep(1)
        if not data:
            return None
        return float(data[0]["lat"]), float(data[0]["lon"])

    def _road_distance_km(self, lat_a: float, lon_a: float, lat_b: float, lon_b: float) -> float | None:
        url = (
            "https://router.project-osrm.org/route/v1/driving/"
            f"{lon_a},{lat_a};{lon_b},{lat_b}"
        )
        response = requests.get(url, params={"overview": "false"}, timeout=30)
        response.raise_for_status()
        data = response.json()
        routes = data.get("routes", [])
        if not routes:
            return None
        return round(routes[0]["distance"] / 1000, 2)

    def _same_country(self, country_a: str | None, country_b: str | None) -> bool:
        if is_blank(country_a) or is_blank(country_b):
            return False
        return self._normalize_country(country_a) == self._normalize_country(country_b)

    def _normalize_country(self, value: str | None) -> str:
        norm = self._normalize_text(value)
        return self.country_aliases.get(norm, norm)

    def _default_airport_for_country(self, country: str) -> dict | None:
        country_norm = self._normalize_country(country)
        country_match = self.master_df[self.master_df["country"].astype(str).map(self._normalize_country) == country_norm]
        if country_match.empty:
            return None
        country_match = country_match.sort_values(by=["city", "airport_name"])
        return country_match.iloc[0].to_dict()

    def process(
        self,
        df: pd.DataFrame,
        composite_mode: str | None = None,
        plant_address: str | None = None,
        plant_city: str | None = None,
        plant_country: str | None = None,
        plant_airport_iata: str | None = None,
    ) -> pd.DataFrame:
        results: list[dict] = []

        mode = (composite_mode or "").strip().lower()
        if mode and mode not in {"upstream", "downstream"}:
            raise ValueError("Modo compuesto inválido. Usa upstream o downstream")

        if mode == "" and not any(col in df.columns for col in ["IATA_origen", "IATA_destino", "Ruta_IATA"]):
            raise ValueError("Faltan columnas requeridas para viaje corporativo: IATA_origen / IATA_destino / Ruta_IATA")

        plant_point = None
        selected_plant_airport = None
        plant_to_airport_km = None

        if mode:
            try:
                plant_point = self._geocode_plant(plant_address, plant_city, plant_country)
            except Exception as exc:
                raise ValueError(f"No se pudo geocodificar la ubicación de la planta: {exc}") from exc
            if plant_point is None:
                raise ValueError("No se pudo geocodificar la ubicación de la planta")

            if not is_blank(plant_airport_iata):
                selected_plant_airport = self._lookup_airport(str(plant_airport_iata).strip().upper())
                if selected_plant_airport is None:
                    raise ValueError("Aeropuerto de planta no encontrado")
            elif not is_blank(plant_country):
                selected_plant_airport = self._default_airport_for_country(str(plant_country))

            if selected_plant_airport is None:
                raise ValueError("No se encontró aeropuerto principal para el país de la planta")
            if not self._same_country(selected_plant_airport.get("country"), plant_country):
                raise ValueError("El aeropuerto de planta debe estar en el mismo país que la planta")

            if mode == "downstream":
                try:
                    plant_to_airport_km = self._road_distance_km(
                        plant_point[0],
                        plant_point[1],
                        float(selected_plant_airport["lat"]),
                        float(selected_plant_airport["lon"]),
                    )
                except Exception as exc:
                    raise ValueError(f"No se pudo calcular distancia planta-aeropuerto: {exc}") from exc

        for _, row in df.iterrows():
            if mode == "upstream":
                origin_airport = self._resolve_row_airport(row, "origin")
                if origin_airport is None:
                    results.append({**row.to_dict(), "Estado": "IATA/CIUDAD ORIGEN NO ENCONTRADO"})
                    continue
                route_codes = [origin_airport["iata_norm"], selected_plant_airport["iata_norm"]]
            elif mode == "downstream":
                destination_airport = self._resolve_row_airport(row, "destination")
                if destination_airport is None:
                    results.append({**row.to_dict(), "Estado": "IATA/CIUDAD DESTINO NO ENCONTRADO"})
                    continue
                route_codes = [selected_plant_airport["iata_norm"], destination_airport["iata_norm"]]
            else:
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
            result_row = {
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

            if mode == "upstream" and plant_point and last_airport:
                result_row["Modo_compuesto"] = "upstream"
                result_row["Aeropuerto_llegada_planta"] = last_airport.get("iata_norm")
                result_row["Ciudad_llegada_planta"] = last_airport.get("city")
                result_row["Pais_llegada_planta"] = last_airport.get("country")

                try:
                    airport_to_plant = self._road_distance_km(
                        float(last_airport["lat"]),
                        float(last_airport["lon"]),
                        plant_point[0],
                        plant_point[1],
                    )
                except Exception:
                    airport_to_plant = None
                result_row["Distancia Aeropuerto - Planta"] = airport_to_plant
                result_row["Distancia_total_compuesta_km"] = round(total_distance + (airport_to_plant or 0), 2)

            if mode == "downstream" and plant_point and selected_plant_airport and last_airport:
                air_from_plant_airport = haversine_km(
                    float(selected_plant_airport["lat"]),
                    float(selected_plant_airport["lon"]),
                    float(last_airport["lat"]),
                    float(last_airport["lon"]),
                )
                result_row["Modo_compuesto"] = "downstream"
                result_row["Aeropuerto_salida_planta"] = selected_plant_airport["iata_norm"]
                result_row["Ciudad_salida_planta"] = selected_plant_airport.get("city")
                result_row["Pais_salida_planta"] = selected_plant_airport.get("country")
                result_row["Aeropuerto_llegada"] = last_airport.get("iata_norm")
                result_row["Ciudad_llegada"] = last_airport.get("city")
                result_row["Pais_llegada"] = last_airport.get("country")
                result_row["Distancia a Aeropuerto"] = plant_to_airport_km
                result_row["Distancia_aerea_desde_planta_km"] = round(air_from_plant_airport, 2)
                result_row["Distancia_total_compuesta_km"] = round((plant_to_airport_km or 0) + air_from_plant_airport, 2)

            results.append(result_row)

        return pd.DataFrame(results)
