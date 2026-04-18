import pandas as pd

from app.main import healthcheck
from app.services.iata_service import IATAService
from app.services.terrestre_ruta_service import TerrestreRutaService
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


def test_iata_service_computes_distance_for_known_codes():
    service = IATAService()
    df = pd.DataFrame([{"IATA_origen": "SCL", "IATA_destino": "LIM"}])

    result = service.process(df)

    assert result.iloc[0]["Estado"] == "OK"
    assert result.iloc[0]["Distancia_km_aerea"] > 0


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
    result = service.process(df)

    assert result.iloc[0]["Estado"] == "COORDENADAS INVÁLIDAS"
