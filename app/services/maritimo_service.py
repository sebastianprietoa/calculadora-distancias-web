from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
import re

import pandas as pd

from app.utils.text import normalize_text
from app.utils.validators import is_blank

BASE_DIR = Path(__file__).resolve().parents[2]
ASSETS_DIR = BASE_DIR / "assets"
MASTER_GLOB = "consolidado_distancias_origen*.xlsx"
MASTER_SHEET = "Consolidado_Pares"
NM_TO_KM = 1.852


@dataclass
class PortResolution:
    port: dict | None
    method: str | None = None
    score: float | None = None
    note: str | None = None
    query: str | None = None
    error: str | None = None


class MaritimoService:
    country_alias_overrides = {
        "ae": "AE",
        "alemania": "DE",
        "argentina": "AR",
        "austria": "AT",
        "belgica": "BE",
        "belgique": "BE",
        "belgie": "BE",
        "belgium": "BE",
        "bolivia": "BO",
        "brasil": "BR",
        "brazil": "BR",
        "ca": "CA",
        "canada": "CA",
        "chile": "CL",
        "china": "CN",
        "colombia": "CO",
        "de": "DE",
        "deutschland": "DE",
        "eeuu": "US",
        "emiratos arabes unidos": "AE",
        "es": "ES",
        "espana": "ES",
        "estados unidos": "US",
        "francia": "FR",
        "france": "FR",
        "germany": "DE",
        "great britain": "GB",
        "holanda": "NL",
        "it": "IT",
        "italia": "IT",
        "italy": "IT",
        "jp": "JP",
        "japon": "JP",
        "japan": "JP",
        "mx": "MX",
        "mexico": "MX",
        "nederland": "NL",
        "netherlands": "NL",
        "nl": "NL",
        "paises bajos": "NL",
        "paises bajos reino de los": "NL",
        "panama": "PA",
        "pa": "PA",
        "pe": "PE",
        "peru": "PE",
        "peru republica del": "PE",
        "portugal": "PT",
        "reino unido": "GB",
        "ru": "RU",
        "rusia": "RU",
        "russia": "RU",
        "saudi arabia": "SA",
        "spain": "ES",
        "suiza": "CH",
        "switzerland": "CH",
        "turkey": "TR",
        "turquia": "TR",
        "uae": "AE",
        "uk": "GB",
        "united arab emirates": "AE",
        "united kingdom": "GB",
        "united states": "US",
        "united states of america": "US",
        "us": "US",
        "usa": "US",
    }
    principal_port_overrides = {
        "AR": "ARBUE",
        "BE": "BEANR",
        "BR": "BRPNG",
        "CL": "CLSAI",
        "CN": "CNSHG",
        "ES": "ESCAR",
        "MX": "MXZLO",
        "NL": "NLAMS",
        "PE": "PECLL",
        "US": "USNYC",
    }

    def __init__(self, pairs_df: pd.DataFrame | None = None) -> None:
        self.raw_pairs_df = pairs_df.copy() if pairs_df is not None else self._load_pairs_from_workbook()
        self.pairs_df = self._prepare_pairs_df(self.raw_pairs_df)
        self.ports_df = self._build_ports_catalog(self.raw_pairs_df)
        self.country_aliases = self._build_country_aliases(self.ports_df, self.raw_pairs_df)
        self.country_codes = set(self.ports_df["country_code"].dropna().astype(str).str.upper())
        self.pair_lookup = self.pairs_df.set_index("pair_key_norm", drop=False)

    def _load_pairs_from_workbook(self) -> pd.DataFrame:
        matches = sorted(ASSETS_DIR.glob(MASTER_GLOB))
        if not matches:
            raise FileNotFoundError(f"No se encontro un archivo {MASTER_GLOB} en {ASSETS_DIR}")

        workbook_path = matches[0]
        excel = pd.ExcelFile(workbook_path)
        sheet_name = self._match_sheet_name(excel.sheet_names, MASTER_SHEET)
        return pd.read_excel(workbook_path, sheet_name=sheet_name)

    def _match_sheet_name(self, sheet_names: list[str], expected: str) -> str:
        expected_norm = self._normalize_text(expected).replace(" ", "")
        for sheet_name in sheet_names:
            candidate_norm = self._normalize_text(sheet_name).replace(" ", "")
            if candidate_norm == expected_norm:
                return sheet_name
        raise ValueError(f"No se encontro la hoja '{expected}' en el archivo maritimo")

    def _prepare_pairs_df(self, df: pd.DataFrame) -> pd.DataFrame:
        working = df.copy()
        working["status_norm"] = self._column(working, "status").astype(str).str.strip().str.lower()
        working["origin_portcode_norm"] = self._column(working, "origin_portcode").map(self._normalize_portcode)
        working["destination_portcode_norm"] = self._column(working, "destination_portcode").map(self._normalize_portcode)
        working["pair_key_norm"] = (
            working["origin_portcode_norm"] + "__" + working["destination_portcode_norm"]
        )
        working["distance_nm"] = pd.to_numeric(self._column(working, "distance_nm"), errors="coerce")
        working["distance_km"] = (working["distance_nm"] * NM_TO_KM).round(2)

        ok_pairs = working[
            (working["status_norm"] == "ok")
            & working["distance_nm"].notna()
            & (working["origin_portcode_norm"] != "")
            & (working["destination_portcode_norm"] != "")
        ].copy()
        return ok_pairs.drop_duplicates(subset=["pair_key_norm"], keep="first")

    def _build_ports_catalog(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        working = raw_df.copy()
        working["status_norm"] = self._column(working, "status").astype(str).str.strip().str.lower()
        working["distance_nm_num"] = pd.to_numeric(self._column(working, "distance_nm"), errors="coerce")

        origin_ports = pd.DataFrame(
            {
                "portcode": self._column(working, "origin_portcode"),
                "port_name": self._column(working, "origin_port_name"),
                "country_code": self._column(working, "origin_country_code"),
                "country": self._column(working, "origin_country"),
                "partner_portcode": self._column(working, "destination_portcode"),
                "ok_record": (working["status_norm"] == "ok") & working["distance_nm_num"].notna(),
                "function": None,
            }
        )
        destination_ports = pd.DataFrame(
            {
                "portcode": self._column(working, "destination_portcode"),
                "port_name": self._column(working, "destination_port_name"),
                "country_code": self._column(working, "destination_country_code"),
                "country": self._column(working, "destination_country"),
                "partner_portcode": self._column(working, "origin_portcode"),
                "ok_record": (working["status_norm"] == "ok") & working["distance_nm_num"].notna(),
                "function": self._column(working, "Function"),
            }
        )

        ports = pd.concat([origin_ports, destination_ports], ignore_index=True)
        ports["portcode_norm"] = ports["portcode"].map(self._normalize_portcode)
        ports["partner_portcode_norm"] = ports["partner_portcode"].map(self._normalize_portcode)
        ports["country_code"] = ports["country_code"].astype(str).str.strip().str.upper()
        ports["function_score"] = ports["function"].map(self._function_score)
        ports = ports[ports["portcode_norm"] != ""].copy()

        rows: list[dict] = []
        for portcode_norm, group in ports.groupby("portcode_norm", dropna=False):
            ok_group = group[group["ok_record"] == True]
            port_name = self._most_common_text(group["port_name"])
            country = self._most_common_text(group["country"])
            country_code = self._most_common_text(group["country_code"]).upper()

            rows.append(
                {
                    "portcode": self._most_common_text(group["portcode"]).upper(),
                    "portcode_norm": portcode_norm,
                    "port_name": port_name,
                    "country_code": country_code,
                    "country": country,
                    "country_norm": self._normalize_text(country),
                    "all_records": int(len(group)),
                    "ok_records": int(group["ok_record"].fillna(False).sum()),
                    "partner_count": int(ok_group["partner_portcode_norm"].replace("", pd.NA).dropna().nunique()),
                    "function_score": int(group["function_score"].fillna(0).max()),
                    "name_aliases": tuple(sorted(self._port_name_aliases(port_name))),
                    "sort_name": self._normalize_text(port_name),
                }
            )

        return pd.DataFrame(rows)

    def _build_country_aliases(self, ports_df: pd.DataFrame, raw_df: pd.DataFrame) -> dict[str, str]:
        aliases: dict[str, str] = {}

        for _, row in ports_df.iterrows():
            country_code = str(row.get("country_code") or "").strip().upper()
            country_name = str(row.get("country") or "").strip()
            if country_code:
                aliases[self._normalize_text(country_code)] = country_code
            if country_name:
                aliases[self._normalize_text(country_name)] = country_code

        if "Country" in raw_df.columns and "CountryName" in raw_df.columns:
            metadata = raw_df[["Country", "CountryName"]].dropna(how="all").drop_duplicates()
            for _, row in metadata.iterrows():
                country_code = str(row.get("Country") or "").strip().upper()
                country_name = str(row.get("CountryName") or "").strip()
                if country_code:
                    aliases[self._normalize_text(country_code)] = country_code
                if country_name:
                    aliases[self._normalize_text(country_name)] = country_code

        for alias, country_code in self.country_alias_overrides.items():
            aliases[self._normalize_text(alias)] = country_code

        return aliases

    def _column(self, df: pd.DataFrame, name: str, default: object = "") -> pd.Series:
        if name in df.columns:
            return df[name]
        return pd.Series([default] * len(df), index=df.index)

    def _normalize_text(self, value: object) -> str:
        text = normalize_text("" if value is None else str(value))
        text = re.sub(r"[\W_]+", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _normalize_header(self, value: object) -> str:
        return self._normalize_text(value).replace(" ", "")

    def _normalize_portcode(self, value: object) -> str:
        if is_blank(value):
            return ""
        return re.sub(r"[^A-Za-z0-9]", "", str(value).strip().upper())

    def _most_common_text(self, values: pd.Series) -> str:
        cleaned = [str(value).strip() for value in values if not is_blank(value)]
        if not cleaned:
            return ""
        counts = pd.Series(cleaned).value_counts()
        best = counts.index[0]
        return str(best).strip()

    def _function_score(self, value: object) -> int:
        if is_blank(value):
            return 0
        return sum(1 for ch in str(value) if ch.isdigit())

    def _port_name_aliases(self, port_name: str) -> set[str]:
        if is_blank(port_name):
            return set()

        raw = str(port_name).strip()
        aliases = {
            self._normalize_text(raw),
            self._normalize_text(re.sub(r"\([^)]*\)", " ", raw)),
        }
        for piece in re.findall(r"\(([^)]*)\)", raw):
            aliases.add(self._normalize_text(piece))
        for piece in re.split(r"[/,;]", raw):
            aliases.add(self._normalize_text(piece))

        expanded: set[str] = set()
        for alias in aliases:
            if not alias:
                continue
            expanded.add(alias)
            stripped = re.sub(r"\b(port|puerto|pt)\b", " ", alias)
            stripped = re.sub(r"\s+", " ", stripped).strip()
            if stripped:
                expanded.add(stripped)
        return {alias for alias in expanded if alias}

    def _similarity(self, left: str, right: str) -> float:
        if not left or not right:
            return 0.0
        if left == right:
            return 100.0
        if left in right or right in left:
            shorter = min(len(left), len(right))
            longer = max(len(left), len(right))
            return round(90.0 + (shorter / longer) * 10.0, 2)
        return round(SequenceMatcher(None, left, right).ratio() * 100.0, 2)

    def _resolve_country_code(self, value: object) -> tuple[str | None, float | None]:
        if is_blank(value):
            return None, None

        norm = self._normalize_text(value)
        if len(norm) == 2:
            country_code = norm.upper()
            if country_code in self.country_codes:
                return country_code, 100.0

        exact = self.country_aliases.get(norm)
        if exact:
            return exact, 100.0

        best_code = None
        best_score = 0.0
        for alias, country_code in self.country_aliases.items():
            score = self._similarity(norm, alias)
            if score > best_score:
                best_score = score
                best_code = country_code

        if best_code and best_score >= 84.0:
            return best_code, best_score
        return None, None

    def _row_value(self, row: pd.Series, candidates: list[str]) -> str | None:
        normalized_lookup = {
            self._normalize_header(column): column
            for column in row.index
        }
        for candidate in candidates:
            actual_column = normalized_lookup.get(self._normalize_header(candidate))
            if actual_column is not None and not is_blank(row[actual_column]):
                return str(row[actual_column]).strip()
        return None

    def _resolve_by_code(self, code: str | None, label: str) -> PortResolution:
        if is_blank(code):
            return PortResolution(port=None, error="FALTAN DATOS")

        code_norm = self._normalize_portcode(code)
        if len(code_norm) != 5:
            return PortResolution(
                port=None,
                method="codigo",
                query=str(code).strip(),
                error=f"FORMATO CODIGO PUERTO {label} INVALIDO",
            )

        matches = self.ports_df[self.ports_df["portcode_norm"] == code_norm]
        if matches.empty:
            return PortResolution(
                port=None,
                method="codigo",
                query=str(code).strip(),
                error=f"CODIGO PUERTO {label} NO ENCONTRADO",
            )

        return PortResolution(
            port=matches.iloc[0].to_dict(),
            method="codigo",
            score=100.0,
            query=code_norm,
        )

    def _default_port_for_country(self, country_code: str) -> dict | None:
        country_ports = self.ports_df[self.ports_df["country_code"] == country_code].copy()
        if country_ports.empty:
            return None

        ok_ports = country_ports[country_ports["ok_records"] > 0].copy()
        ranked = ok_ports if not ok_ports.empty else country_ports

        override_code = self.principal_port_overrides.get(country_code)
        if override_code:
            override_match = ranked[ranked["portcode_norm"] == self._normalize_portcode(override_code)]
            if not override_match.empty:
                return override_match.iloc[0].to_dict()

        ranked = ranked.sort_values(
            by=["function_score", "partner_count", "ok_records", "all_records", "sort_name"],
            ascending=[False, False, False, False, True],
        )
        return ranked.iloc[0].to_dict()

    def _lookup_port_by_city_country(
        self,
        city: str,
        country_code: str,
    ) -> PortResolution:
        city_norm = self._normalize_text(city)
        country_ports = self.ports_df[self.ports_df["country_code"] == country_code].copy()
        if country_ports.empty:
            return PortResolution(port=None, error="PUERTO/CIUDAD NO ENCONTRADO")

        best_row: dict | None = None
        best_score = 0.0

        for _, port in country_ports.iterrows():
            aliases = port.get("name_aliases") or ()
            port_best = 0.0
            for alias in aliases:
                port_best = max(port_best, self._similarity(city_norm, str(alias)))
                if port_best == 100.0:
                    break

            if port_best > best_score:
                best_row = port.to_dict()
                best_score = port_best
            elif best_row is not None and port_best == best_score:
                candidate_tuple = (
                    port.get("function_score", 0),
                    port.get("partner_count", 0),
                    port.get("ok_records", 0),
                    port.get("all_records", 0),
                    port.get("sort_name", ""),
                )
                current_tuple = (
                    best_row.get("function_score", 0),
                    best_row.get("partner_count", 0),
                    best_row.get("ok_records", 0),
                    best_row.get("all_records", 0),
                    best_row.get("sort_name", ""),
                )
                if candidate_tuple > current_tuple:
                    best_row = port.to_dict()

        if best_row and best_score >= 84.0:
            return PortResolution(
                port=best_row,
                method="ciudad_pais",
                score=best_score,
                query=f"{city}, {country_code}",
            )

        return PortResolution(port=None, error="PUERTO/CIUDAD NO ENCONTRADO")

    def _resolve_by_text(self, city: str | None, country: str | None, label: str) -> PortResolution:
        if is_blank(city) and is_blank(country):
            return PortResolution(port=None, error="FALTAN DATOS")
        if is_blank(country):
            return PortResolution(port=None, error=f"FALTA PAIS {label}")

        country_code, _country_score = self._resolve_country_code(country)
        if country_code is None:
            return PortResolution(port=None, error=f"PAIS {label} NO ENCONTRADO")

        if is_blank(city):
            default_port = self._default_port_for_country(country_code)
            if default_port is None:
                return PortResolution(port=None, error=f"PUERTO PRINCIPAL {label} NO ENCONTRADO")
            return PortResolution(
                port=default_port,
                method="pais_principal",
                score=100.0,
                query=str(country).strip(),
                note=f"Se uso el puerto principal de {default_port.get('country') or country_code}",
            )

        result = self._lookup_port_by_city_country(str(city).strip(), country_code)
        result.query = f"{str(city).strip()}, {str(country).strip()}"
        if result.port is None and result.error == "PUERTO/CIUDAD NO ENCONTRADO":
            result.error = f"PUERTO/CIUDAD {label} NO ENCONTRADO"
        return result

    def _join_note(self, first: str | None, second: str | None) -> str | None:
        items = [item for item in [first, second] if item]
        if not items:
            return None
        return " | ".join(items)

    def _resolve_row_port(self, row: pd.Series, kind: str) -> PortResolution:
        label = "ORIGEN" if kind == "origin" else "DESTINO"
        if kind == "origin":
            code = self._row_value(
                row,
                [
                    "origin_portcode",
                    "Codigo_puerto_origen",
                    "Codigo puerto origen",
                    "Puerto_origen_codigo",
                    "Puerto origen codigo",
                    "Portcode_origen",
                    "Portcode origen",
                ],
            )
            city = self._row_value(
                row,
                [
                    "origin_port_name",
                    "Ciudad_origen",
                    "Puerto_origen",
                    "Port_origen",
                    "Port origin",
                ],
            )
            country = self._row_value(
                row,
                [
                    "origin_country",
                    "origin_country_code",
                    "Pais_origen",
                    "Country_origen",
                    "Country origin",
                ],
            )
        else:
            code = self._row_value(
                row,
                [
                    "destination_portcode",
                    "Codigo_puerto_destino",
                    "Puerto_destino_codigo",
                    "Portcode_destino",
                    "Portcode destination",
                ],
            )
            city = self._row_value(
                row,
                [
                    "destination_port_name",
                    "Ciudad_destino",
                    "Puerto_destino",
                    "Port_destino",
                    "Port destination",
                ],
            )
            country = self._row_value(
                row,
                [
                    "destination_country",
                    "destination_country_code",
                    "Pais_destino",
                    "Country_destino",
                    "Country destination",
                ],
            )

        has_text_fallback = not is_blank(country) or not is_blank(city)
        if not is_blank(code):
            code_result = self._resolve_by_code(code, label)
            if code_result.port is not None or not has_text_fallback:
                return code_result

            text_result = self._resolve_by_text(city, country, label)
            if text_result.port is not None:
                text_result.note = self._join_note(
                    text_result.note,
                    f"Se ignoro el codigo de puerto ingresado: {code_result.error.lower()}",
                )
                return text_result

            code_result.error = self._join_note(code_result.error, text_result.error)
            return code_result

        return self._resolve_by_text(city, country, label)

    def _lookup_pair(self, origin_portcode: str, destination_portcode: str) -> tuple[dict | None, str]:
        direct_key = f"{self._normalize_portcode(origin_portcode)}__{self._normalize_portcode(destination_portcode)}"
        if direct_key in self.pair_lookup.index:
            return self.pair_lookup.loc[direct_key].to_dict(), "directo"

        reverse_key = f"{self._normalize_portcode(destination_portcode)}__{self._normalize_portcode(origin_portcode)}"
        if reverse_key in self.pair_lookup.index:
            return self.pair_lookup.loc[reverse_key].to_dict(), "invertido"

        return None, "sin_match"

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            raise ValueError("El archivo no contiene filas para procesar")

        results: list[dict] = []

        for _, row in df.iterrows():
            origin = self._resolve_row_port(row, "origin")
            destination = self._resolve_row_port(row, "destination")

            result_row = {
                **row.to_dict(),
                "Consulta_origen": origin.query,
                "Consulta_destino": destination.query,
                "Metodo_resolucion_origen": origin.method,
                "Metodo_resolucion_destino": destination.method,
                "Coincidencia_origen_pct": origin.score,
                "Coincidencia_destino_pct": destination.score,
                "Observacion_origen": origin.note,
                "Observacion_destino": destination.note,
                "Puerto_origen_codigo_resuelto": origin.port.get("portcode") if origin.port else None,
                "Puerto_origen_resuelto": origin.port.get("port_name") if origin.port else None,
                "Pais_origen_resuelto": origin.port.get("country") if origin.port else None,
                "Puerto_destino_codigo_resuelto": destination.port.get("portcode") if destination.port else None,
                "Puerto_destino_resuelto": destination.port.get("port_name") if destination.port else None,
                "Pais_destino_resuelto": destination.port.get("country") if destination.port else None,
                "Pair_key_resuelto": None,
                "Sentido_lookup": None,
                "Observacion_lookup": None,
                "Distancia_nm": None,
                "Distancia_km": None,
            }

            errors = [error for error in [origin.error, destination.error] if error]
            if errors:
                result_row["Estado"] = " | ".join(errors)
                results.append(result_row)
                continue

            pair, lookup_direction = self._lookup_pair(
                origin.port["portcode"],
                destination.port["portcode"],
            )
            if pair is None:
                result_row["Estado"] = "DISTANCIA MARITIMA NO ENCONTRADA"
                results.append(result_row)
                continue

            note = None
            if lookup_direction == "invertido":
                note = "Se uso el par invertido disponible en el catalogo"

            result_row["Pair_key_resuelto"] = pair.get("pair_key") or pair.get("pair_key_norm")
            result_row["Sentido_lookup"] = lookup_direction
            result_row["Observacion_lookup"] = note
            result_row["Distancia_nm"] = round(float(pair["distance_nm"]), 2)
            result_row["Distancia_km"] = round(float(pair["distance_km"]), 2)
            result_row["Estado"] = "OK"
            results.append(result_row)

        return pd.DataFrame(results)
