from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
import math
import re

import pandas as pd

from app.utils.text import normalize_text
from app.utils.validators import is_blank

BASE_DIR = Path(__file__).resolve().parents[2]
ASSETS_DIR = BASE_DIR / "assets"
CONSOLIDATED_GLOB = "consolidado_distancias_origen*.xlsx"
CONSOLIDATED_SHEET = "Consolidado_Pares"
CITY_MASTER_FILE = ASSETS_DIR / "Distancias maestras v1.xlsx"
NM_TO_KM = 1.852


@dataclass
class LocationResolution:
    city: str | None = None
    country: str | None = None
    country_token: str | None = None
    port: dict | None = None
    method: str | None = None
    score: float | None = None
    note: str | None = None
    query: str | None = None
    error: str | None = None


@dataclass
class DistanceResolution:
    row: dict | None
    kind: str | None = None
    note: str | None = None


class MaritimoService:
    country_alias_overrides = {
        "ae": "AE",
        "albania": "AL",
        "alemania": "DE",
        "arabia saudita": "SA",
        "argentina": "AR",
        "austria": "AT",
        "bahrain": "BH",
        "bangladesh": "BD",
        "barain": "BH",
        "belgica": "BE",
        "belgique": "BE",
        "belgie": "BE",
        "belgium": "BE",
        "bolivia": "BO",
        "brasil": "BR",
        "brazil": "BR",
        "brunei": "BN",
        "brunei darussalam": "BN",
        "ca": "CA",
        "canada": "CA",
        "chile": "CL",
        "china": "CN",
        "chipre": "CY",
        "colombia": "CO",
        "corea del sur": "KR",
        "costa rica": "CR",
        "cyprus": "CY",
        "de": "DE",
        "deutschland": "DE",
        "dinamarca": "DK",
        "do": "DO",
        "ecuador": "EC",
        "ee uu": "US",
        "eeuu": "US",
        "emiratos arabes unidos": "AE",
        "espana": "ES",
        "estados unidos": "US",
        "estados unides": "US",
        "france": "FR",
        "francia": "FR",
        "germany": "DE",
        "great britain": "GB",
        "holanda": "NL",
        "hong kong": "HK",
        "hong kong sar": "HK",
        "india": "IN",
        "indonesia": "ID",
        "inglaterra": "GB",
        "israel": "IL",
        "italia": "IT",
        "italy": "IT",
        "japan": "JP",
        "japon": "JP",
        "jordania": "JO",
        "korea": "KR",
        "korea south": "KR",
        "kuwait": "KW",
        "kuwuait": "KW",
        "libano": "LB",
        "lithuania": "LT",
        "lituania": "LT",
        "malasia": "MY",
        "mexico": "MX",
        "moldavia": "MD",
        "moldova republic of": "MD",
        "mx": "MX",
        "nederland": "NL",
        "netherlands": "NL",
        "nl": "NL",
        "pa": "PA",
        "pakistan": "PK",
        "panama": "PA",
        "paises bajos": "NL",
        "paises bajos reino de los": "NL",
        "paraguay": "PY",
        "pe": "PE",
        "peru": "PE",
        "peru republica de": "PE",
        "peru republica del": "PE",
        "philippines": "PH",
        "poland": "PL",
        "polonia": "PL",
        "portugal": "PT",
        "qatar": "QA",
        "reino unido": "GB",
        "rep dominicana": "DO",
        "republica dominicana": "DO",
        "romania": "RO",
        "rumania": "RO",
        "rusia": "RU",
        "russia": "RU",
        "saudi arabia": "SA",
        "singapur": "SG",
        "singapore": "SG",
        "south korea": "KR",
        "spain": "ES",
        "suiza": "CH",
        "switzerland": "CH",
        "taiwan": "TW",
        "tailandia": "TH",
        "thailand": "TH",
        "turkey": "TR",
        "turquia": "TR",
        "uae": "AE",
        "uk": "GB",
        "united arab emirates": "AE",
        "united kingdom": "GB",
        "united states": "US",
        "united states of america": "US",
        "uruguay": "UY",
        "us": "US",
        "usa": "US",
        "venezuela": "VE",
        "venezuela bolivarian republic of": "VE",
        "vietnam": "VN",
    }
    principal_port_overrides = {
        "AR": "ARBUE",
        "BE": "BEANR",
        "BR": "BRPNG",
        "CL": "CLSAI",
        "CN": "CNSHG",
        "ES": "ESCAR",
        "HK": "HKHKG",
        "MX": "MXZLO",
        "NL": "NLAMS",
        "PE": "PECLL",
        "SG": "SGSIN",
        "US": "USNYC",
    }

    def __init__(
        self,
        pairs_df: pd.DataFrame | None = None,
        city_master_df: pd.DataFrame | None = None,
    ) -> None:
        self.country_aliases: dict[str, str] = {}
        self.country_display_names: dict[str, str] = {}

        for alias, token in self.country_alias_overrides.items():
            self._register_country_alias(alias, token)

        self.raw_pairs_df = pairs_df.copy() if pairs_df is not None else self._load_pairs_from_workbook()
        self.pairs_df = self._prepare_pairs_df(self.raw_pairs_df)
        self.ports_df = self._build_ports_catalog(self.raw_pairs_df)
        self._register_country_aliases_from_pairs(self.ports_df, self.raw_pairs_df)

        self.primary_city_by_country = self._build_primary_city_map()

        self.city_master_df = city_master_df.copy() if city_master_df is not None else self._load_city_master_workbook()
        self.city_pairs_df = self._build_city_pairs_catalog(self.city_master_df)
        self.city_pair_lookup = self.city_pairs_df.set_index("pair_key_norm", drop=False)
        self.pair_lookup = self.pairs_df.set_index("pair_key_norm", drop=False)

    def _load_pairs_from_workbook(self) -> pd.DataFrame:
        matches = sorted(ASSETS_DIR.glob(CONSOLIDATED_GLOB))
        if not matches:
            raise FileNotFoundError(f"No se encontro un archivo {CONSOLIDATED_GLOB} en {ASSETS_DIR}")

        workbook_path = matches[0]
        excel = pd.ExcelFile(workbook_path)
        sheet_name = self._match_sheet_name(excel.sheet_names, CONSOLIDATED_SHEET)
        return pd.read_excel(workbook_path, sheet_name=sheet_name)

    def _load_city_master_workbook(self) -> pd.DataFrame:
        if not CITY_MASTER_FILE.exists():
            return pd.DataFrame()
        return pd.read_excel(CITY_MASTER_FILE)

    def _match_sheet_name(self, sheet_names: list[str], expected: str) -> str:
        expected_norm = self._normalize_header(expected)
        for sheet_name in sheet_names:
            if self._normalize_header(sheet_name) == expected_norm:
                return sheet_name
        raise ValueError(f"No se encontro la hoja '{expected}' en el archivo maritimo")

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

    def _normalize_city_key(self, value: object) -> str:
        text = self._normalize_text(value)
        text = re.sub(r"\b(pto|puerto|port|aerop|aeropuerto)\b", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _normalize_portcode(self, value: object) -> str:
        if is_blank(value):
            return ""
        return re.sub(r"[^A-Za-z0-9]", "", str(value).strip().upper())

    def _display_country_name(self, token: str | None) -> str | None:
        if token is None or pd.isna(token):
            return None
        token = str(token).strip()
        if not token:
            return None
        name = self.country_display_names.get(token)
        if name:
            return name
        if len(token) == 2 and token.isupper():
            return token
        return " ".join(piece.capitalize() for piece in token.split())

    def _register_country_alias(self, alias: object, token: str | None, display_name: object | None = None) -> None:
        if token is None:
            return
        alias_norm = self._normalize_text(alias)
        if alias_norm:
            self.country_aliases[alias_norm] = token

        display = None if display_name is None else str(display_name).strip()
        if display:
            current = self.country_display_names.get(token)
            if current is None or len(current) <= 2:
                self.country_display_names[token] = display

    def _resolve_country_token(self, value: object, allow_fallback: bool = False) -> tuple[str | None, float | None]:
        if is_blank(value):
            return None, None

        raw = str(value).strip()
        norm = self._normalize_text(raw)
        if not norm:
            return None, None

        if len(norm) == 2:
            token = norm.upper()
            self._register_country_alias(raw, token, raw.upper())
            return token, 100.0

        exact = self.country_aliases.get(norm)
        if exact:
            self._register_country_alias(raw, exact, raw)
            return exact, 100.0

        best_token = None
        best_score = 0.0
        for alias, token in self.country_aliases.items():
            score = self._similarity(norm, alias)
            if score > best_score:
                best_score = score
                best_token = token

        if best_token and best_score >= 84.0:
            self._register_country_alias(raw, best_token, raw)
            return best_token, best_score

        if allow_fallback:
            fallback = norm
            self._register_country_alias(raw, fallback, raw)
            return fallback, 70.0

        return None, None

    def _register_country_aliases_from_pairs(self, ports_df: pd.DataFrame, raw_df: pd.DataFrame) -> None:
        for _, row in ports_df.iterrows():
            country_code = str(row.get("country_code") or "").strip().upper()
            country_name = str(row.get("country") or "").strip()
            if country_code:
                self._register_country_alias(country_code, country_code, country_name or country_code)
            if country_name:
                self._register_country_alias(country_name, country_code or country_name, country_name)

        if "Country" in raw_df.columns and "CountryName" in raw_df.columns:
            metadata = raw_df[["Country", "CountryName"]].dropna(how="all").drop_duplicates()
            for _, row in metadata.iterrows():
                country_code = str(row.get("Country") or "").strip().upper()
                country_name = str(row.get("CountryName") or "").strip()
                if country_code:
                    self._register_country_alias(country_code, country_code, country_name or country_code)
                if country_name:
                    self._register_country_alias(country_name, country_code or country_name, country_name)

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
                    "port_name_key": self._normalize_city_key(port_name),
                    "country_code": country_code,
                    "country": country,
                    "all_records": int(len(group)),
                    "ok_records": int(group["ok_record"].fillna(False).sum()),
                    "partner_count": int(ok_group["partner_portcode_norm"].replace("", pd.NA).dropna().nunique()),
                    "function_score": int(group["function_score"].fillna(0).max()),
                    "name_aliases": tuple(sorted(self._port_name_aliases(port_name))),
                    "sort_name": self._normalize_text(port_name),
                }
            )

        return pd.DataFrame(rows)

    def _build_primary_city_map(self) -> dict[str, dict[str, str]]:
        primary_map: dict[str, dict[str, str]] = {}
        for country_token in sorted(self.ports_df["country_code"].dropna().astype(str).str.upper().unique()):
            port = self._default_port_for_country(country_token)
            if port is None:
                continue
            primary_map[country_token] = {
                "city": str(port.get("port_name") or "").strip(),
                "city_key": self._normalize_city_key(port.get("port_name")),
                "country": str(port.get("country") or "").strip(),
            }
        return primary_map

    def _build_consolidated_city_pairs(self) -> pd.DataFrame:
        if self.pairs_df.empty:
            return pd.DataFrame()

        working = self.pairs_df.copy()
        working["origin_country_token"] = working["origin_country_code"].astype(str).str.upper()
        working["destination_country_token"] = working["destination_country_code"].astype(str).str.upper()
        working["origin_city_key"] = working["origin_port_name"].map(self._normalize_city_key)
        working["destination_city_key"] = working["destination_port_name"].map(self._normalize_city_key)
        working["pair_key_city_norm"] = (
            working["origin_country_token"]
            + "|"
            + working["origin_city_key"]
            + "||"
            + working["destination_country_token"]
            + "|"
            + working["destination_city_key"]
        )

        rows: list[dict] = []
        for pair_key_norm, group in working.groupby("pair_key_city_norm", dropna=False):
            rows.append(
                {
                    "pair_key_norm": pair_key_norm,
                    "origin_city": self._most_common_text(group["origin_port_name"]),
                    "origin_city_key": self._most_common_text(group["origin_city_key"]),
                    "origin_country": self._display_country_name(self._most_common_text(group["origin_country_token"]))
                    or self._most_common_text(group["origin_country"]),
                    "origin_country_token": self._most_common_text(group["origin_country_token"]),
                    "destination_city": self._most_common_text(group["destination_port_name"]),
                    "destination_city_key": self._most_common_text(group["destination_city_key"]),
                    "destination_country": self._display_country_name(
                        self._most_common_text(group["destination_country_token"])
                    ) or self._most_common_text(group["destination_country"]),
                    "destination_country_token": self._most_common_text(group["destination_country_token"]),
                    "distance_km": round(float(group["distance_km"].median()), 2),
                    "distance_nm": round(float(group["distance_nm"].median()), 2),
                    "source": "consolidado",
                    "validation_mode": "consolidado",
                    "support_rows": int(len(group)),
                    "source_priority": 3,
                }
            )

        result = pd.DataFrame(rows)
        result["origin_is_primary"] = result.apply(
            lambda row: self._is_primary_city(row["origin_country_token"], row["origin_city_key"]), axis=1
        )
        result["destination_is_primary"] = result.apply(
            lambda row: self._is_primary_city(row["destination_country_token"], row["destination_city_key"]), axis=1
        )
        return result

    def _master_header_map(self, df: pd.DataFrame) -> dict[str, str]:
        return {self._normalize_header(column): column for column in df.columns}

    def _pick_master_column(
        self,
        df: pd.DataFrame,
        header_map: dict[str, str],
        normalized_name: str,
        default: object = "",
    ) -> pd.Series:
        actual = header_map.get(normalized_name)
        if actual is None:
            return pd.Series([default] * len(df), index=df.index)
        return df[actual]

    def _build_city_pairs_catalog(self, city_master_df: pd.DataFrame) -> pd.DataFrame:
        consolidated = self._build_consolidated_city_pairs()
        if city_master_df.empty:
            return consolidated

        header_map = self._master_header_map(city_master_df)
        master = pd.DataFrame(
            {
                "origin_country_raw": self._pick_master_column(city_master_df, header_map, "paisdeorigen"),
                "origin_city_raw": self._pick_master_column(city_master_df, header_map, "ciudadorigen"),
                "destination_country_raw": self._pick_master_column(city_master_df, header_map, "paisdedestino"),
                "destination_city_raw": self._pick_master_column(city_master_df, header_map, "ciudaddestino"),
                "distance_km": pd.to_numeric(
                    self._pick_master_column(city_master_df, header_map, "distancia"),
                    errors="coerce",
                ),
            }
        )
        master = master.dropna(subset=["distance_km"]).copy()

        master = self._repair_master_rows(master)
        city_country_map = self._build_unique_city_country_map(master, consolidated)
        master = self._fill_master_countries(master, city_country_map)
        master_pairs = self._aggregate_master_pairs(master)
        master_pairs = self._filter_master_pairs_by_corridor(master_pairs, consolidated)

        if master_pairs.empty:
            return consolidated

        consolidated_keys = set(consolidated["pair_key_norm"]) if not consolidated.empty else set()
        master_new = master_pairs[~master_pairs["pair_key_norm"].isin(consolidated_keys)].copy()
        combined = pd.concat([consolidated, master_new], ignore_index=True)
        combined["origin_is_primary"] = combined.apply(
            lambda row: self._is_primary_city(row["origin_country_token"], row["origin_city_key"]), axis=1
        )
        combined["destination_is_primary"] = combined.apply(
            lambda row: self._is_primary_city(row["destination_country_token"], row["destination_city_key"]), axis=1
        )
        return combined

    def _repair_master_rows(self, master: pd.DataFrame) -> pd.DataFrame:
        for side in ["origin", "destination"]:
            city_col = f"{side}_city_raw"
            country_col = f"{side}_country_raw"

            city_tokens = master[city_col].map(lambda value: self._resolve_country_token(value)[0])
            country_tokens = master[country_col].map(lambda value: self._resolve_country_token(value)[0])

            swap_mask = city_tokens.notna() & country_tokens.isna() & master[country_col].notna()
            if swap_mask.any():
                swap_values = master.loc[swap_mask, [country_col, city_col]].to_numpy()
                master.loc[swap_mask, [city_col, country_col]] = swap_values

            master[f"{side}_city_key"] = master[city_col].map(self._normalize_city_key)
            master[f"{side}_country_token"] = master[country_col].map(
                lambda value: self._resolve_country_token(value, allow_fallback=True)[0]
            )

        return master

    def _build_unique_city_country_map(self, master: pd.DataFrame, consolidated: pd.DataFrame) -> dict[str, str]:
        city_frames = [
            master[["origin_city_key", "origin_country_token"]].rename(
                columns={"origin_city_key": "city_key", "origin_country_token": "country_token"}
            ),
            master[["destination_city_key", "destination_country_token"]].rename(
                columns={"destination_city_key": "city_key", "destination_country_token": "country_token"}
            ),
        ]
        if not consolidated.empty:
            city_frames.extend(
                [
                    consolidated[["origin_city_key", "origin_country_token"]].rename(
                        columns={"origin_city_key": "city_key", "origin_country_token": "country_token"}
                    ),
                    consolidated[["destination_city_key", "destination_country_token"]].rename(
                        columns={"destination_city_key": "city_key", "destination_country_token": "country_token"}
                    ),
                ]
            )

        city_map = pd.concat(city_frames, ignore_index=True)
        city_map = city_map[(city_map["city_key"] != "") & city_map["country_token"].notna()].copy()
        counts = city_map.groupby("city_key")["country_token"].nunique().reset_index(name="country_count")
        unique_city_keys = set(counts[counts["country_count"] == 1]["city_key"])

        lookup = (
            city_map[city_map["city_key"].isin(unique_city_keys)]
            .drop_duplicates("city_key")
            .set_index("city_key")["country_token"]
            .to_dict()
        )
        return lookup

    def _fill_master_countries(self, master: pd.DataFrame, city_country_map: dict[str, str]) -> pd.DataFrame:
        for side in ["origin", "destination"]:
            raw_country_col = f"{side}_country_raw"
            city_raw_col = f"{side}_city_raw"
            city_key_col = f"{side}_city_key"
            token_col = f"{side}_country_token"

            missing = master[token_col].isna() | master[token_col].eq("")
            master.loc[missing, token_col] = master.loc[missing, city_key_col].map(city_country_map).fillna("")

            city_as_country = master[city_raw_col].map(lambda value: self._resolve_country_token(value)[0])
            missing = master[token_col].eq("")
            master.loc[missing & city_as_country.notna(), token_col] = city_as_country[missing & city_as_country.notna()]
            master.loc[
                missing & city_as_country.notna() & master[raw_country_col].isna(),
                raw_country_col,
            ] = master.loc[missing & city_as_country.notna(), city_raw_col]

            master[token_col] = master[token_col].replace("", pd.NA)
            master[f"{side}_country"] = master.apply(
                lambda row: self._display_country_name(row[token_col]) or str(row[raw_country_col]).strip() or None,
                axis=1,
            )
            master[f"{side}_city"] = master[city_raw_col].astype(str).str.strip()

        master = master.dropna(
            subset=[
                "origin_country_token",
                "destination_country_token",
                "origin_city_key",
                "destination_city_key",
            ]
        ).copy()
        master = master[(master["origin_city_key"] != "") & (master["destination_city_key"] != "")]
        return master

    def _aggregate_master_pairs(self, master: pd.DataFrame) -> pd.DataFrame:
        if master.empty:
            return pd.DataFrame()

        master["pair_key_norm"] = (
            master["origin_country_token"]
            + "|"
            + master["origin_city_key"]
            + "||"
            + master["destination_country_token"]
            + "|"
            + master["destination_city_key"]
        )
        master["country_pair_key"] = master["origin_country_token"] + "||" + master["destination_country_token"]

        rows: list[dict] = []
        for pair_key_norm, group in master.groupby("pair_key_norm", dropna=False):
            distance_km, validation_mode, kept_rows, spread_km = self._select_master_distance(group["distance_km"])
            if distance_km is None or validation_mode == "suspect":
                continue

            rows.append(
                {
                    "pair_key_norm": pair_key_norm,
                    "origin_city": self._most_common_text(group["origin_city"]),
                    "origin_city_key": self._most_common_text(group["origin_city_key"]),
                    "origin_country": self._most_common_text(group["origin_country"]),
                    "origin_country_token": self._most_common_text(group["origin_country_token"]),
                    "destination_city": self._most_common_text(group["destination_city"]),
                    "destination_city_key": self._most_common_text(group["destination_city_key"]),
                    "destination_country": self._most_common_text(group["destination_country"]),
                    "destination_country_token": self._most_common_text(group["destination_country_token"]),
                    "distance_km": round(distance_km, 2),
                    "distance_nm": None,
                    "country_pair_key": self._most_common_text(group["country_pair_key"]),
                    "source": f"maestra_{validation_mode}",
                    "validation_mode": validation_mode,
                    "support_rows": kept_rows,
                    "spread_km": spread_km,
                    "source_priority": 2 if validation_mode == "cluster" else 1,
                }
            )

        return pd.DataFrame(rows)

    def _filter_master_pairs_by_corridor(self, master_pairs: pd.DataFrame, consolidated: pd.DataFrame) -> pd.DataFrame:
        if master_pairs.empty:
            return master_pairs

        master_pairs = master_pairs.copy()
        reference = pd.concat(
            [
                consolidated.assign(country_pair_key=lambda df: df["origin_country_token"] + "||" + df["destination_country_token"])[
                    ["country_pair_key", "distance_km"]
                ],
                master_pairs[master_pairs["validation_mode"] == "cluster"][["country_pair_key", "distance_km"]],
            ],
            ignore_index=True,
        )
        reference = reference.dropna(subset=["country_pair_key", "distance_km"])
        corridor_stats = reference.groupby("country_pair_key").agg(
            corridor_count=("distance_km", "size"),
            corridor_median=("distance_km", "median"),
        )

        if corridor_stats.empty:
            return master_pairs

        checked = master_pairs.join(corridor_stats, on="country_pair_key")
        single_mask = checked["validation_mode"] == "single"
        ref_mask = checked["corridor_count"].fillna(0) >= 3
        checked["corridor_ratio"] = checked["distance_km"] / checked["corridor_median"]

        rejected_mask = (
            single_mask
            & ref_mask
            & ((checked["corridor_ratio"] < 0.55) | (checked["corridor_ratio"] > 1.65))
        )
        return checked[~rejected_mask].copy()

    def _select_master_distance(self, values: pd.Series) -> tuple[float | None, str, int, float | None]:
        distances = sorted(float(value) for value in values if pd.notna(value))
        if not distances:
            return None, "empty", 0, None
        if len(distances) == 1:
            return distances[0], "single", 1, 0.0

        median = float(pd.Series(distances).median())
        tolerance = max(250.0, median * 0.08)
        kept = [value for value in distances if abs(value - median) <= tolerance]

        if len(kept) >= max(2, math.ceil(len(distances) / 2)):
            spread = max(kept) - min(kept)
            return float(pd.Series(kept).median()), "cluster", len(kept), spread

        return median, "suspect", 1, max(distances) - min(distances)

    def _most_common_text(self, values: pd.Series) -> str:
        cleaned = [str(value).strip() for value in values if not is_blank(value)]
        if not cleaned:
            return ""
        counts = pd.Series(cleaned).value_counts()
        return str(counts.index[0]).strip()

    def _function_score(self, value: object) -> int:
        if is_blank(value):
            return 0
        return sum(1 for char in str(value) if char.isdigit())

    def _port_name_aliases(self, port_name: str) -> set[str]:
        if is_blank(port_name):
            return set()

        raw = str(port_name).strip()
        aliases = {
            self._normalize_city_key(raw),
            self._normalize_city_key(re.sub(r"\([^)]*\)", " ", raw)),
        }
        for piece in re.findall(r"\(([^)]*)\)", raw):
            aliases.add(self._normalize_city_key(piece))
        for piece in re.split(r"[/,;]", raw):
            aliases.add(self._normalize_city_key(piece))
        return {alias for alias in aliases if alias}

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

    def _row_value(self, row: pd.Series, candidates: list[str]) -> str | None:
        normalized_lookup = {self._normalize_header(column): column for column in row.index}
        for candidate in candidates:
            actual_column = normalized_lookup.get(self._normalize_header(candidate))
            if actual_column is not None and not is_blank(row[actual_column]):
                return str(row[actual_column]).strip()
        return None

    def _default_port_for_country(self, country_token: str) -> dict | None:
        country_ports = self.ports_df[self.ports_df["country_code"] == country_token].copy()
        if country_ports.empty:
            return None

        ok_ports = country_ports[country_ports["ok_records"] > 0].copy()
        ranked = ok_ports if not ok_ports.empty else country_ports

        override_code = self.principal_port_overrides.get(country_token)
        if override_code:
            override_match = ranked[ranked["portcode_norm"] == self._normalize_portcode(override_code)]
            if not override_match.empty:
                return override_match.iloc[0].to_dict()

        ranked = ranked.sort_values(
            by=["function_score", "partner_count", "ok_records", "all_records", "sort_name"],
            ascending=[False, False, False, False, True],
        )
        return ranked.iloc[0].to_dict()

    def _is_primary_city(self, country_token: str | None, city_key: str | None) -> bool:
        if not country_token or not city_key:
            return False
        primary = self.primary_city_by_country.get(country_token)
        return primary is not None and primary["city_key"] == city_key

    def _resolve_by_code(self, code: str | None, label: str) -> LocationResolution:
        if is_blank(code):
            return LocationResolution(error="FALTAN DATOS")

        code_norm = self._normalize_portcode(code)
        if len(code_norm) != 5:
            return LocationResolution(
                method="codigo",
                query=str(code).strip(),
                error=f"FORMATO CODIGO PUERTO {label} INVALIDO",
            )

        matches = self.ports_df[self.ports_df["portcode_norm"] == code_norm]
        if matches.empty:
            return LocationResolution(
                method="codigo",
                query=str(code).strip(),
                error=f"CODIGO PUERTO {label} NO ENCONTRADO",
            )

        port = matches.iloc[0].to_dict()
        return LocationResolution(
            city=port.get("port_name"),
            country=port.get("country"),
            country_token=port.get("country_code"),
            port=port,
            method="codigo",
            score=100.0,
            query=code_norm,
        )

    def _resolve_by_text(self, city: str | None, country: str | None, label: str) -> LocationResolution:
        if is_blank(city) and is_blank(country):
            return LocationResolution(error="FALTAN DATOS")
        if is_blank(country):
            return LocationResolution(error=f"FALTA PAIS {label}")

        country_token, country_score = self._resolve_country_token(country)
        if country_token is None:
            return LocationResolution(error=f"PAIS {label} NO ENCONTRADO")

        city_value = None if is_blank(city) else str(city).strip()
        country_value = self._display_country_name(country_token) or str(country).strip()
        note = None
        method = "ciudad_pais" if city_value else "pais"

        default_port = None
        if city_value is None:
            default_port = self._default_port_for_country(country_token)
            if default_port is not None:
                city_value = default_port.get("port_name")
                country_value = default_port.get("country") or country_value
                note = f"Se uso el puerto principal de {country_value}"
                method = "pais_principal"

        return LocationResolution(
            city=city_value,
            country=country_value,
            country_token=country_token,
            port=default_port,
            method=method,
            score=country_score or 100.0,
            note=note,
            query=f"{city_value}, {country_value}" if city_value else str(country).strip(),
        )

    def _extract_row_inputs(self, row: pd.Series, kind: str) -> tuple[str | None, str | None, str | None]:
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
                    "Ciudad origen",
                    "Puerto_origen",
                    "Puerto origen",
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
                    "País_origen",
                    "Pais origen",
                    "País origen",
                    "Country_origen",
                    "Country origin",
                ],
            )
            return code, city, country

        code = self._row_value(
            row,
            [
                "destination_portcode",
                "Codigo_puerto_destino",
                "Codigo puerto destino",
                "Puerto_destino_codigo",
                "Puerto destino codigo",
                "Portcode_destino",
                "Portcode destino",
            ],
        )
        city = self._row_value(
            row,
            [
                "destination_port_name",
                "Ciudad_destino",
                "Ciudad destino",
                "Puerto_destino",
                "Puerto destino",
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
                "País_destino",
                "Pais destino",
                "País destino",
                "Country_destino",
                "Country destination",
            ],
        )
        return code, city, country

    def _resolve_row_location(self, row: pd.Series, kind: str) -> LocationResolution:
        label = "ORIGEN" if kind == "origin" else "DESTINO"
        code, city, country = self._extract_row_inputs(row, kind)

        has_text_fallback = not is_blank(city) or not is_blank(country)
        if not is_blank(code):
            code_result = self._resolve_by_code(code, label)
            if code_result.error is None or not has_text_fallback:
                return code_result

            text_result = self._resolve_by_text(city, country, label)
            if text_result.error is None:
                text_result.note = self._join_note(
                    text_result.note,
                    f"Se ignoro el codigo de puerto ingresado: {code_result.error.lower()}",
                )
                return text_result

            code_result.error = self._join_note(code_result.error, text_result.error)
            return code_result

        return self._resolve_by_text(city, country, label)

    def _lookup_city_distance(self, origin: LocationResolution, destination: LocationResolution) -> DistanceResolution:
        if not origin.country_token or not destination.country_token:
            return DistanceResolution(row=None)

        candidates = self.city_pairs_df[
            (self.city_pairs_df["origin_country_token"] == origin.country_token)
            & (self.city_pairs_df["destination_country_token"] == destination.country_token)
        ].copy()
        if candidates.empty:
            return DistanceResolution(row=None)

        origin_key = self._normalize_city_key(origin.city)
        destination_key = self._normalize_city_key(destination.city)

        if origin_key and destination_key:
            direct_key = (
                origin.country_token + "|" + origin_key + "||" + destination.country_token + "|" + destination_key
            )
            if direct_key in self.city_pair_lookup.index:
                return DistanceResolution(row=self.city_pair_lookup.loc[direct_key].to_dict(), kind="Exacta")

        candidates["origin_score"] = candidates["origin_city_key"].map(
            lambda value: self._similarity(origin_key, value) if origin_key else 0.0
        )
        candidates["destination_score"] = candidates["destination_city_key"].map(
            lambda value: self._similarity(destination_key, value) if destination_key else 0.0
        )

        if not origin_key:
            candidates["origin_score"] = candidates["origin_is_primary"].map(lambda value: 100.0 if value else 75.0)
        if not destination_key:
            candidates["destination_score"] = candidates["destination_is_primary"].map(lambda value: 100.0 if value else 75.0)

        exact = candidates.copy()
        if origin_key:
            exact = exact[exact["origin_score"] >= 84.0]
        if destination_key:
            exact = exact[exact["destination_score"] >= 84.0]
        if not exact.empty:
            exact["priority"] = (
                exact["origin_score"]
                + exact["destination_score"]
                + exact["source_priority"] * 8
                + exact["support_rows"].clip(upper=5)
            )
            best_exact = exact.sort_values("priority", ascending=False).iloc[0].to_dict()
            note = self._exact_distance_note(origin_key, destination_key, best_exact)
            return DistanceResolution(row=best_exact, kind="Exacta", note=note)

        proxy = candidates.copy()
        if origin_key:
            proxy = proxy[proxy["origin_score"] >= 55.0]
        if destination_key:
            close_proxy = proxy[proxy["destination_score"] >= 45.0]
            if not close_proxy.empty:
                proxy = close_proxy
        if proxy.empty:
            proxy = candidates.copy()

        proxy["priority"] = (
            proxy["origin_score"] * 1.35
            + proxy["destination_score"] * 1.2
            + proxy["source_priority"] * 10
            + proxy["origin_is_primary"].map(lambda value: 4 if value else 0)
            + proxy["destination_is_primary"].map(lambda value: 6 if value else 0)
            + proxy["support_rows"].clip(upper=5)
        )
        best_proxy = proxy.sort_values("priority", ascending=False).iloc[0].to_dict()
        return DistanceResolution(
            row=best_proxy,
            kind="Distancia Proxy",
            note=self._proxy_distance_note(origin, destination, best_proxy),
        )

    def _exact_distance_note(
        self,
        origin_key: str,
        destination_key: str,
        row: dict,
    ) -> str | None:
        notes: list[str] = []
        if origin_key and row.get("origin_city_key") != origin_key:
            notes.append(f"Se ajusto la ciudad origen a {row.get('origin_city')}")
        if destination_key and row.get("destination_city_key") != destination_key:
            notes.append(f"Se ajusto la ciudad destino a {row.get('destination_city')}")
        if not notes:
            return None
        return " | ".join(notes)

    def _proxy_distance_note(
        self,
        origin: LocationResolution,
        destination: LocationResolution,
        row: dict,
    ) -> str:
        notes: list[str] = []
        origin_key = self._normalize_city_key(origin.city)
        destination_key = self._normalize_city_key(destination.city)

        if row.get("origin_city_key") != origin_key:
            notes.append(f"Origen proxy: {row.get('origin_city')}, {row.get('origin_country')}")
        if row.get("destination_city_key") != destination_key:
            notes.append(f"Destino proxy: {row.get('destination_city')}, {row.get('destination_country')}")
        if not notes:
            notes.append("Se uso un par proxy dentro del mismo pais")
        return " | ".join(notes)

    def _lookup_pair(self, origin_portcode: str, destination_portcode: str) -> tuple[dict | None, str]:
        direct_key = f"{self._normalize_portcode(origin_portcode)}__{self._normalize_portcode(destination_portcode)}"
        if direct_key in self.pair_lookup.index:
            return self.pair_lookup.loc[direct_key].to_dict(), "directo"

        reverse_key = f"{self._normalize_portcode(destination_portcode)}__{self._normalize_portcode(origin_portcode)}"
        if reverse_key in self.pair_lookup.index:
            return self.pair_lookup.loc[reverse_key].to_dict(), "invertido"

        return None, "sin_match"

    def _join_note(self, first: str | None, second: str | None) -> str | None:
        notes = [item for item in [first, second] if item]
        if not notes:
            return None
        return " | ".join(notes)

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            raise ValueError("El archivo no contiene filas para procesar")

        results: list[dict] = []

        for _, row in df.iterrows():
            origin = self._resolve_row_location(row, "origin")
            destination = self._resolve_row_location(row, "destination")

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
                "Ciudad_origen_resuelta": origin.city,
                "Pais_origen_resuelto": origin.country,
                "Ciudad_destino_resuelta": destination.city,
                "Pais_destino_resuelto": destination.country,
                "Puerto_origen_codigo_resuelto": origin.port.get("portcode") if origin.port else None,
                "Puerto_destino_codigo_resuelto": destination.port.get("portcode") if destination.port else None,
                "Pair_key_resuelto": None,
                "Tipo_distancia": None,
                "Fuente_distancia": None,
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

            distance = self._lookup_city_distance(origin, destination)
            if distance.row is not None:
                distance_row = distance.row
                result_row["Ciudad_origen_resuelta"] = distance_row.get("origin_city") or result_row["Ciudad_origen_resuelta"]
                result_row["Pais_origen_resuelto"] = distance_row.get("origin_country") or result_row["Pais_origen_resuelto"]
                result_row["Ciudad_destino_resuelta"] = distance_row.get("destination_city") or result_row["Ciudad_destino_resuelta"]
                result_row["Pais_destino_resuelto"] = distance_row.get("destination_country") or result_row["Pais_destino_resuelto"]
                result_row["Pair_key_resuelto"] = distance_row.get("pair_key_norm")
                result_row["Tipo_distancia"] = distance.kind
                result_row["Fuente_distancia"] = distance_row.get("source")
                result_row["Observacion_lookup"] = distance.note
                result_row["Distancia_nm"] = (
                    round(float(distance_row["distance_nm"]), 2)
                    if pd.notna(distance_row.get("distance_nm"))
                    else None
                )
                result_row["Distancia_km"] = round(float(distance_row["distance_km"]), 2)
                result_row["Estado"] = "OK"
                results.append(result_row)
                continue

            if origin.port is not None and destination.port is not None:
                pair, lookup_direction = self._lookup_pair(origin.port["portcode"], destination.port["portcode"])
                if pair is not None:
                    result_row["Ciudad_origen_resuelta"] = origin.port.get("port_name")
                    result_row["Pais_origen_resuelto"] = origin.port.get("country")
                    result_row["Ciudad_destino_resuelta"] = destination.port.get("port_name")
                    result_row["Pais_destino_resuelto"] = destination.port.get("country")
                    result_row["Pair_key_resuelto"] = pair.get("pair_key") or pair.get("pair_key_norm")
                    result_row["Tipo_distancia"] = "Exacta"
                    result_row["Fuente_distancia"] = "consolidado_portcode"
                    result_row["Sentido_lookup"] = lookup_direction
                    result_row["Observacion_lookup"] = (
                        "Se uso el par invertido disponible en el catalogo"
                        if lookup_direction == "invertido"
                        else None
                    )
                    result_row["Distancia_nm"] = round(float(pair["distance_nm"]), 2)
                    result_row["Distancia_km"] = round(float(pair["distance_km"]), 2)
                    result_row["Estado"] = "OK"
                    results.append(result_row)
                    continue

            result_row["Estado"] = "DISTANCIA MARITIMA NO ENCONTRADA"
            results.append(result_row)

        return pd.DataFrame(results)
