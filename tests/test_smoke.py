import asyncio
from io import BytesIO

import pandas as pd
from fastapi import UploadFile

from app.main import healthcheck
from app.routes.coordenadas import _json_safe_df, _safe_json_float
from app.services.coordenadas_service import CoordenadasService
from app.services.iata_service import IATAService
from app.services.terrestre_ruta_service import TerrestreRutaService
from app.utils.excel import dataframe_to_excel_bytes, read_uploaded_table, validate_extension
from app.utils.validators import parse_float_in_range, require_columns


def test_healthcheck_ok():
    assert healthcheck() == {"status": "ok"}


def test_require_columns_raises_missing_column():
    df = pd.DataFrame([{"Ciudad": "Santiago"}])
    try:
        require_columns(df, ["Ciudad", "País"])
        assert False, "require_columns debía lanzar ValueError"
    except ValueError as exc:
        assert "País" in str(exc)


def test_validate_extension_accepts_csv_xlsx_and_rejects_other():
    assert validate_extension("input.csv") == ".csv"
    assert validate_extension("input.xlsx") == ".xlsx"

    try:
        validate_extension("input.txt")
        assert False, "validate_extension debía lanzar ValueError"
    except ValueError:
        assert True


def test_iata_service_reports_blank_and_invalid_format():
    service = IATAService()
    df = pd.DataFrame(
        [
            {"IATA_origen": " ", "IATA_destino": "LIM"},
            {"IATA_origen": "SC", "IATA_destino": "LIM"},
        ]
    )

    result = service.process(df)

    assert result.iloc[0]["Estado"] == "FALTAN DATOS"
    assert result.iloc[1]["Estado"] == "FORMATO IATA INVÁLIDO"


def test_iata_service_normalizes_and_computes_distance_for_known_codes():
    service = IATAService()
    df = pd.DataFrame([{"IATA_origen": " scl ", "IATA_destino": "lim"}])

    result = service.process(df)

    assert result.iloc[0]["Estado"] == "OK"
    assert result.iloc[0]["IATA_origen_norm"] == "SCL"
    assert result.iloc[0]["IATA_destino_norm"] == "LIM"
    assert result.iloc[0]["Distancia_km"] > 0


def test_coordenadas_service_builds_consulta_from_ciudad_pais(monkeypatch):
    service = CoordenadasService()
    monkeypatch.setattr(service, "_load_cache", lambda: pd.DataFrame(columns=["cache_key", "Latitud", "Longitud", "Display_name"]))
    monkeypatch.setattr(service, "_save_cache", lambda _: None)
    monkeypatch.setattr(
        service,
        "_query_nominatim",
        lambda ciudad, pais: {"lat": "-33.45", "lon": "-70.66", "display_name": f"{ciudad}, {pais}"},
    )

    df = pd.DataFrame([{"Ciudad": "Santiago", "País": "Chile"}])
    result = service.process(df)

    assert result.iloc[0]["Consulta"] == "Santiago, Chile"
    assert result.iloc[0]["Estado"] == "OK"
    assert result.iloc[0]["Precision_pct"] >= 90


def test_parse_float_in_range_rejects_out_of_bounds():
    try:
        parse_float_in_range(120, -90, 90)
        assert False, "parse_float_in_range debía lanzar ValueError"
    except ValueError:
        assert True


def test_terrestre_service_marks_invalid_coordinates_without_calling_osrm(monkeypatch):
    service = TerrestreRutaService()

    def _unexpected(*args, **kwargs):
        raise AssertionError("No debería llamar OSRM para coordenadas inválidas")

    monkeypatch.setattr(service, "_query_osrm", _unexpected)

    df = pd.DataFrame(
        [{"Latitud ori": 95, "Longitud ori": -70, "Latitud des": -12, "Longitud des": -77}]
    )
    result = service.process(df, mode="coordenadas")

    assert result.iloc[0]["Estado"].startswith("ENTRADA INVÁLIDA")


def test_read_uploaded_table_rejects_empty_file():
    upload = UploadFile(filename="empty.csv", file=BytesIO(b""))
    try:
        asyncio.run(read_uploaded_table(upload))
        assert False, "read_uploaded_table debía lanzar ValueError"
    except ValueError as exc:
        assert "vacío" in str(exc)


def test_dataframe_to_excel_bytes_generates_binary_excel():
    df = pd.DataFrame([{"x": 1}])
    output = dataframe_to_excel_bytes(df)

    assert isinstance(output, bytes)
    assert len(output) > 0
    assert output[:2] == b"PK"


def test_read_uploaded_table_accepts_csv_content():
    upload = UploadFile(filename="sample.csv", file=BytesIO("Ciudad,País\nSantiago,Chile\n".encode("utf-8")))
    df = asyncio.run(read_uploaded_table(upload))

    assert list(df.columns) == ["Ciudad", "País"]
    assert df.iloc[0]["Ciudad"] == "Santiago"


def test_json_safe_df_replaces_nan_and_inf():
    df = pd.DataFrame([{"valor": float("inf"), "otro": float("nan")}])
    safe = _json_safe_df(df)

    assert safe.iloc[0]["valor"] is None
    assert safe.iloc[0]["otro"] is None


def test_safe_json_float_handles_inf_nan_and_invalid_values():
    assert _safe_json_float(float("inf")) == 0.0
    assert _safe_json_float(float("nan")) == 0.0
    assert _safe_json_float("abc") == 0.0
    assert _safe_json_float(12.345) == 12.35


def test_iata_service_supports_route_and_sums_segments():
    service = IATAService()
    route_df = pd.DataFrame([{"IATA_origen": "LIM/SCL/LIM", "IATA_destino": None}])
    pair_df = pd.DataFrame([{"IATA_origen": "LIM", "IATA_destino": "SCL"}])

    route_result = service.process(route_df).iloc[0]
    pair_result = service.process(pair_df).iloc[0]

    assert route_result["Estado"] == "OK"
    assert route_result["Tramos_calculados"] == 2
    assert route_result["Ruta_IATA_norm"] == "LIM/SCL/LIM"
    assert route_result["Distancia_km"] == round(pair_result["Distancia_km"] * 2, 2)


def test_terrestre_service_text_mode_uses_country_capital_when_city_missing(monkeypatch):
    service = TerrestreRutaService()

    calls = []

    def _fake_geocode(query):
        calls.append(query)
        if "capital de Chile" in query:
            return (-33.45, -70.66)
        if "capital de Peru" in query:
            return (-12.0464, -77.0428)
        return None

    monkeypatch.setattr(service, "_geocode", _fake_geocode)
    monkeypatch.setattr(service, "_query_osrm", lambda *args, **kwargs: {"distance": 10000, "duration": 1200})

    df = pd.DataFrame([{"Pais ori": "Chile", "Pais des": "Peru"}])
    result = service.process(df, mode="direccion")

    assert result.iloc[0]["Estado"] == "OK"
    assert result.iloc[0]["Modo_entrada"] == "direccion"
    assert "capital de Chile" in result.iloc[0]["Consulta_ori"]
    assert "capital de Peru" in result.iloc[0]["Consulta_des"]
    assert any("capital de Chile" in q for q in calls)
