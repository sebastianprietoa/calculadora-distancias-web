from __future__ import annotations

from pathlib import Path
from time import sleep

import pandas as pd
import requests

from app.utils.text import normalize_text
from app.utils.validators import is_blank, require_columns

BASE_DIR = Path(__file__).resolve().parents[2]
CACHE_FILE = BASE_DIR / "data" / "cache" / "geocache.csv"
USER_AGENT = "calculadora-distancias-web/0.1 (internal testing)"


class CoordenadasService:
    required_columns = ["Ciudad", "País"]

    def __init__(self) -> None:
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not CACHE_FILE.exists():
            pd.DataFrame(
                columns=["cache_key", "Latitud", "Longitud", "Display_name"]
            ).to_csv(CACHE_FILE, index=False)

    def _load_cache(self) -> pd.DataFrame:
        return pd.read_csv(CACHE_FILE)

    def _save_cache(self, cache_df: pd.DataFrame) -> None:
        cache_df.to_csv(CACHE_FILE, index=False)

    def _build_cache_key(self, ciudad: str, pais: str) -> str:
        return f"{normalize_text(ciudad)}|{normalize_text(pais)}"

    def _query_nominatim(self, ciudad: str, pais: str) -> dict | None:
        url = "https://nominatim.openstreetmap.org/search"
        params = {
            "q": f"{ciudad}, {pais}",
            "format": "jsonv2",
            "limit": 1,
        }
        response = requests.get(
            url,
            params=params,
            headers={"User-Agent": USER_AGENT},
            timeout=20,
        )
        response.raise_for_status()
        data = response.json()
        sleep(1)
        if not data:
            return None
        return data[0]


    def _estimate_precision_pct(self, ciudad: str, pais: str, display_name: str | None, fuente: str | None) -> float:
        if not display_name:
            return 0.0

        display_norm = normalize_text(str(display_name))
        ciudad_norm = normalize_text(ciudad)
        pais_norm = normalize_text(pais)

        score = 0.0
        if ciudad_norm and ciudad_norm in display_norm:
            score += 50.0
        if pais_norm and pais_norm in display_norm:
            score += 40.0
        if fuente == "cache":
            score += 10.0

        return round(min(score, 100.0), 2)

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        require_columns(df, self.required_columns)
        cache_df = self._load_cache()
        results: list[dict] = []

        for _, row in df.iterrows():
            ciudad = row.get("Ciudad")
            pais = row.get("País")

            if is_blank(ciudad) or is_blank(pais):
                results.append(
                    {
                        **row.to_dict(),
                        "Consulta": None,
                        "Latitud": None,
                        "Longitud": None,
                        "Display_name": None,
                        "Fuente": None,
                        "Precision_pct": 0.0,
                        "Estado": "FALTAN DATOS",
                    }
                )
                continue

            ciudad_str = str(ciudad).strip()
            pais_str = str(pais).strip()
            consulta = f"{ciudad_str}, {pais_str}"
            cache_key = self._build_cache_key(ciudad_str, pais_str)
            match = cache_df[cache_df["cache_key"] == cache_key]

            if not match.empty:
                match_row = match.iloc[0]
                results.append(
                    {
                        **row.to_dict(),
                        "Consulta": consulta,
                        "Latitud": match_row["Latitud"],
                        "Longitud": match_row["Longitud"],
                        "Display_name": match_row["Display_name"],
                        "Fuente": "cache",
                        "Precision_pct": self._estimate_precision_pct(ciudad_str, pais_str, match_row["Display_name"], "cache"),
                        "Estado": "OK",
                    }
                )
                continue

            try:
                result = self._query_nominatim(ciudad_str, pais_str)
                if result is None:
                    results.append(
                        {
                            **row.to_dict(),
                            "Consulta": consulta,
                            "Latitud": None,
                            "Longitud": None,
                            "Display_name": None,
                            "Fuente": "nominatim",
                            "Precision_pct": 0.0,
                            "Estado": "NO ENCONTRADO",
                        }
                    )
                    continue

                cache_df.loc[len(cache_df)] = {
                    "cache_key": cache_key,
                    "Latitud": result.get("lat"),
                    "Longitud": result.get("lon"),
                    "Display_name": result.get("display_name"),
                }
                self._save_cache(cache_df)
                results.append(
                    {
                        **row.to_dict(),
                        "Consulta": consulta,
                        "Latitud": result.get("lat"),
                        "Longitud": result.get("lon"),
                        "Display_name": result.get("display_name"),
                        "Fuente": "nominatim",
                        "Precision_pct": self._estimate_precision_pct(ciudad_str, pais_str, result.get("display_name"), "nominatim"),
                        "Estado": "OK",
                    }
                )
            except Exception as exc:
                results.append(
                    {
                        **row.to_dict(),
                        "Consulta": consulta,
                        "Latitud": None,
                        "Longitud": None,
                        "Display_name": None,
                        "Fuente": "nominatim",
                        "Precision_pct": 0.0,
                        "Estado": f"ERROR CONSULTA: {exc}",
                    }
                )

        return pd.DataFrame(results)
