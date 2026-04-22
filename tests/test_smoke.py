import asyncio
from io import BytesIO

import pandas as pd
from fastapi import UploadFile

from app.main import healthcheck
from app.routes.coordenadas import _json_safe_df, _safe_json_float
from app.routes.iata import _numeric_series
from app.routes.maritimo import _build_template_df as _build_maritimo_template_df
from app.routes.terrestre_ruta import _build_template_df
from app.services.coordenadas_service import CoordenadasService
from app.services.iata_service import IATAService
from app.services.maritimo_service import MaritimoService
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


def test_iata_numeric_series_handles_missing_columns_and_non_finite_values():
    df_missing = pd.DataFrame([{"x": 1}])
    assert _numeric_series(df_missing, "Distancia_total_compuesta_km").sum() == 0

    df_values = pd.DataFrame([{"Distancia_total_compuesta_km": "inf"}, {"Distancia_total_compuesta_km": 12.5}])
    series = _numeric_series(df_values, "Distancia_total_compuesta_km")
    assert float(series.sum()) == 12.5


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


def test_iata_service_corporativo_supports_origen_destino_and_ruta_mix():
    service = IATAService()
    df = pd.DataFrame(
        [
            {"IATA_origen": "SCL", "IATA_destino": "LIM", "Ruta_IATA": "SCL/LIM"},
            {"IATA_origen": "SCL", "IATA_destino": "LIM", "Ruta_IATA": "SCL/LIM/SCL"},
            {"IATA_origen": "SCL", "IATA_destino": "LIM", "Ruta_IATA": None},
            {"IATA_origen": None, "IATA_destino": None, "Ruta_IATA": "SCL/LIM"},
        ]
    )
    result = service.process(df)

    assert (result["Estado"] == "OK").all()
    assert result.iloc[0]["Ruta_IATA_norm"] == "SCL/LIM"
    assert result.iloc[1]["Ruta_IATA_norm"] == "SCL/LIM/SCL"
    assert result.iloc[2]["Ruta_IATA_norm"] == "SCL/LIM"
    assert result.iloc[3]["Ruta_IATA_norm"] == "SCL/LIM"


def test_iata_service_corporativo_accepts_origen_destino_ruta_headers():
    service = IATAService()
    df = pd.DataFrame(
        [
            {"Origen": "SCL", "Destino": "LIM", "Ruta": ""},
            {"Origen": "", "Destino": "", "Ruta": "SCL/LIM/SCL"},
        ]
    )
    result = service.process(df)

    assert (result["Estado"] == "OK").all()
    assert result.iloc[0]["Ruta_IATA_norm"] == "SCL/LIM"
    assert result.iloc[1]["Ruta_IATA_norm"] == "SCL/LIM/SCL"
    assert result.iloc[0]["Distancia_aerea_km"] > 0


def test_iata_service_falls_back_to_global_iata_catalog_for_corporate_routes():
    service = IATAService()
    df = pd.DataFrame(
        [
            {"Origen": "BOS", "Destino": "JFK", "Ruta": ""},
            {"Origen": "", "Destino": "", "Ruta": "AMS/CDG/TRD"},
        ]
    )
    result = service.process(df)

    assert result.iloc[0]["Estado"] == "OK"
    assert result.iloc[0]["Ruta_IATA_norm"] == "BOS/JFK"
    assert result.iloc[0]["Distancia_km"] > 0
    assert result.iloc[1]["Estado"] == "OK"
    assert result.iloc[1]["Tramos_calculados"] == 2


def test_iata_service_rejects_empty_dataframe():
    service = IATAService()
    df = pd.DataFrame(columns=["Origen", "Destino", "Ruta"])
    try:
        service.process(df)
        assert False, "service.process debía lanzar ValueError para planilla sin filas"
    except ValueError as exc:
        assert "no contiene filas" in str(exc)


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


def test_terrestre_template_builder_supports_both_modes():
    coords_cols = list(_build_template_df("coordenadas").columns)
    text_cols = list(_build_template_df("direccion").columns)
    auto_cols = list(_build_template_df("auto").columns)

    assert coords_cols == ["Latitud ori", "Longitud ori", "Latitud des", "Longitud des"]
    assert text_cols == ["Direccion ori", "Ciudad ori", "Pais ori", "Direccion des", "Ciudad des", "Pais des"]
    assert set(coords_cols).issubset(set(auto_cols))
    assert set(text_cols).issubset(set(auto_cols))


def test_iata_service_composite_upstream_adds_airport_to_plant_distance(monkeypatch):
    service = IATAService()
    monkeypatch.setattr(service, "_geocode_plant", lambda *args, **kwargs: (-33.45, -70.66))
    monkeypatch.setattr(service, "_road_distance_km", lambda *args, **kwargs: 12.5)

    df = pd.DataFrame([{"IATA_origen": "LIM", "IATA_destino": "SCL"}])
    result = service.process(
        df,
        composite_mode="upstream",
        plant_country="Chile",
        plant_city="Santiago",
    ).iloc[0]

    assert result["Modo_compuesto"] == "upstream"
    assert result["Distancia Aeropuerto - Planta"] == 12.5
    assert result["Distancia_total_compuesta_km"] == round(result["Distancia_km"] + 12.5, 2)


def test_iata_service_composite_downstream_adds_plant_to_airport_and_air_leg(monkeypatch):
    service = IATAService()
    monkeypatch.setattr(service, "_geocode_plant", lambda *args, **kwargs: (-33.45, -70.66))
    monkeypatch.setattr(service, "_road_distance_km", lambda *args, **kwargs: 15.0)

    df = pd.DataFrame([{"IATA_origen": "LIM", "IATA_destino": "ANF"}])
    result = service.process(
        df,
        composite_mode="downstream",
        plant_country="Chile",
        plant_airport_iata="SCL",
    ).iloc[0]

    assert result["Modo_compuesto"] == "downstream"
    assert result["Aeropuerto_salida_planta"] == "SCL"
    assert result["Distancia a Aeropuerto"] == 15.0
    assert result["Distancia_total_compuesta_km"] >= 15.0


def test_iata_service_upstream_accepts_city_country_when_iata_missing(monkeypatch):
    service = IATAService()
    monkeypatch.setattr(service, "_geocode_plant", lambda *args, **kwargs: (-33.45, -70.66))
    monkeypatch.setattr(service, "_road_distance_km", lambda *args, **kwargs: 20.0)

    df = pd.DataFrame([{"Ciudad_origen": "Antofagasta", "Pais_origen": "Chile"}])
    result = service.process(
        df,
        composite_mode="upstream",
        plant_country="Chile",
        plant_airport_iata="SCL",
    ).iloc[0]

    assert result["Estado"] == "OK"
    assert result["IATA_origen_norm"] == "ANF"
    assert result["IATA_destino_norm"] == "SCL"
    assert result["Distancia_total_compuesta_km"] >= result["Distancia_km"]


def test_iata_service_supports_pmc_code_alias_in_composite(monkeypatch):
    service = IATAService()
    monkeypatch.setattr(service, "_geocode_plant", lambda *args, **kwargs: (-41.47, -72.94))
    monkeypatch.setattr(service, "_road_distance_km", lambda *args, **kwargs: 8.0)

    df = pd.DataFrame([{"IATA_destino": "ANF"}])
    result = service.process(
        df,
        composite_mode="downstream",
        plant_country="Chile",
        plant_airport_iata="PMC",
    ).iloc[0]

    assert result["Estado"] == "OK"
    assert result["Aeropuerto_salida_planta"] == "PMC"


def test_iata_service_city_country_supports_spanish_country_names(monkeypatch):
    service = IATAService()
    monkeypatch.setattr(service, "_geocode_plant", lambda *args, **kwargs: (-33.45, -70.66))
    monkeypatch.setattr(service, "_road_distance_km", lambda *args, **kwargs: 14.0)

    upstream = pd.DataFrame([{"Ciudad_origen": "Madrid", "Pais_origen": "España"}])
    upstream_result = service.process(
        upstream,
        composite_mode="upstream",
        plant_country="Chile",
        plant_airport_iata="SCL",
    ).iloc[0]

    downstream = pd.DataFrame([{"Ciudad_destino": "Buenos Aires", "Pais_destino": "Argentina"}])
    downstream_result = service.process(
        downstream,
        composite_mode="downstream",
        plant_country="Chile",
        plant_airport_iata="SCL",
    ).iloc[0]

    assert upstream_result["Estado"] == "OK"
    assert upstream_result["IATA_origen_norm"] == "MAD"
    assert downstream_result["Estado"] == "OK"
    assert downstream_result["IATA_destino_norm"] == "EZE"


def _sample_maritimo_pairs_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "pair_key": "ARBUE__CLSAI",
                "origin_portcode": "ARBUE",
                "origin_port_name": "Buenos Aires",
                "origin_country_code": "AR",
                "origin_country": "Argentina",
                "destination_portcode": "CLSAI",
                "destination_port_name": "San Antonio",
                "destination_country_code": "CL",
                "destination_country": "Chile",
                "distance_nm": 2936.86,
                "status": "ok",
                "Function": None,
            },
            {
                "pair_key": "CLSAI__ARBUE",
                "origin_portcode": "CLSAI",
                "origin_port_name": "San Antonio",
                "origin_country_code": "CL",
                "origin_country": "Chile",
                "destination_portcode": "ARBUE",
                "destination_port_name": "Buenos Aires",
                "destination_country_code": "AR",
                "destination_country": "Argentina",
                "distance_nm": 2966.87,
                "status": "ok",
                "Function": None,
            },
            {
                "pair_key": "ARBUE__MXLZC",
                "origin_portcode": "ARBUE",
                "origin_port_name": "Buenos Aires",
                "origin_country_code": "AR",
                "origin_country": "Argentina",
                "destination_portcode": "MXLZC",
                "destination_port_name": "Lázaro Cárdenas",
                "destination_country_code": "MX",
                "destination_country": "Mexico",
                "distance_nm": 6604.03,
                "status": "ok",
                "Function": "1-------",
            },
            {
                "pair_key": "ARBUE__MXZLO",
                "origin_portcode": "ARBUE",
                "origin_port_name": "Buenos Aires",
                "origin_country_code": "AR",
                "origin_country": "Argentina",
                "destination_portcode": "MXZLO",
                "destination_port_name": "Manzanillo",
                "destination_country_code": "MX",
                "destination_country": "Mexico",
                "distance_nm": 6483.03,
                "status": "ok",
                "Function": "1-34----",
            },
            {
                "pair_key": "ARBUE__NLAMS",
                "origin_portcode": "ARBUE",
                "origin_port_name": "Buenos Aires",
                "origin_country_code": "AR",
                "origin_country": "Argentina",
                "destination_portcode": "NLAMS",
                "destination_port_name": "Amsterdam",
                "destination_country_code": "NL",
                "destination_country": "Netherlands",
                "distance_nm": 6379.08,
                "status": "ok",
                "Function": None,
            },
            {
                "pair_key": "ARBUE__NLAML",
                "origin_portcode": "ARBUE",
                "origin_port_name": "Buenos Aires",
                "origin_country_code": "AR",
                "origin_country": "Argentina",
                "destination_portcode": "NLAML",
                "destination_port_name": "Ameland",
                "destination_country_code": "NL",
                "destination_country": "Netherlands",
                "distance_nm": 6300.0,
                "status": "ok",
                "Function": "1--4----",
            },
        ]
    )


def test_maritimo_service_resolves_valid_portcodes():
    service = MaritimoService(pairs_df=_sample_maritimo_pairs_df())
    df = pd.DataFrame([{"Codigo_puerto_origen": "arbue", "Codigo_puerto_destino": "clsai"}])

    result = service.process(df).iloc[0]

    assert result["Estado"] == "OK"
    assert result["Puerto_origen_codigo_resuelto"] == "ARBUE"
    assert result["Puerto_destino_codigo_resuelto"] == "CLSAI"
    assert result["Metodo_resolucion_origen"] == "codigo"
    assert result["Metodo_resolucion_destino"] == "codigo"
    assert result["Distancia_nm"] == 2936.86
    assert result["Distancia_km"] == round(2936.86 * 1.852, 2)


def test_maritimo_service_matches_city_country_with_typos():
    service = MaritimoService(pairs_df=_sample_maritimo_pairs_df())
    df = pd.DataFrame(
        [
            {
                "Ciudad_origen": "Buenos Airez",
                "Pais_origen": "Argentina",
                "Ciudad_destino": "Lazaro Cardenas",
                "Pais_destino": "Mexico",
            }
        ]
    )

    result = service.process(df).iloc[0]

    assert result["Estado"] == "OK"
    assert result["Puerto_origen_codigo_resuelto"] == "ARBUE"
    assert result["Puerto_destino_codigo_resuelto"] == "MXLZC"
    assert result["Metodo_resolucion_origen"] == "ciudad_pais"
    assert result["Metodo_resolucion_destino"] == "ciudad_pais"
    assert result["Coincidencia_origen_pct"] >= 84
    assert result["Coincidencia_destino_pct"] >= 84


def test_maritimo_service_uses_principal_port_when_only_country_is_provided():
    service = MaritimoService(pairs_df=_sample_maritimo_pairs_df())
    df = pd.DataFrame([{"Pais_origen": "Argentina", "Pais_destino": "Nederland"}])

    result = service.process(df).iloc[0]

    assert result["Estado"] == "OK"
    assert result["Puerto_origen_codigo_resuelto"] == "ARBUE"
    assert result["Puerto_destino_codigo_resuelto"] == "NLAMS"
    assert result["Metodo_resolucion_origen"] == "pais_principal"
    assert result["Metodo_resolucion_destino"] == "pais_principal"
    assert "puerto principal" in result["Observacion_origen"].lower()
    assert "puerto principal" in result["Observacion_destino"].lower()


def test_maritimo_service_falls_back_to_city_country_when_portcode_is_invalid():
    service = MaritimoService(pairs_df=_sample_maritimo_pairs_df())
    df = pd.DataFrame(
        [
            {
                "Codigo_puerto_origen": "ARB",
                "Ciudad_origen": "Buenos Aires",
                "Pais_origen": "Argentina",
                "Codigo_puerto_destino": "XXXXX",
                "Ciudad_destino": "San Antonio",
                "Pais_destino": "Chile",
            }
        ]
    )

    result = service.process(df).iloc[0]

    assert result["Estado"] == "OK"
    assert result["Puerto_origen_codigo_resuelto"] == "ARBUE"
    assert result["Puerto_destino_codigo_resuelto"] == "CLSAI"
    assert "codigo de puerto" in result["Observacion_origen"].lower()
    assert "codigo de puerto" in result["Observacion_destino"].lower()


def test_maritimo_template_builder_contains_expected_columns():
    columns = list(_build_maritimo_template_df().columns)
    assert columns == [
        "Codigo_puerto_origen",
        "Ciudad_origen",
        "Pais_origen",
        "Codigo_puerto_destino",
        "Ciudad_destino",
        "Pais_destino",
    ]
