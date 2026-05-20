"""
Tests para el módulo localization (Geolocalización).

Verifica:
- Enriquecimiento de dimensión de ubicación con datos GeoIP
- Manejo de IPs inválidas/privadas
- Manejo de errores de archivo no encontrado
- Correcta integración con MaxMind GeoLite2
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock
import geoip2.errors

from src.localization import enrich_location_dimension, _skip_geolocation_ip, _main_cli


# ---------------------------------------------------------------------------
# Fixtures locales
# ---------------------------------------------------------------------------

@pytest.fixture()
def patch_paths(mocker, tmp_path):
    """
    Parchea las constantes Path del módulo hacia directorios temporales.
    Devuelve un dict con las rutas para que los tests puedan escribir
    la entrada y leer la salida sin conocer tmp_path directamente.
    """
    paths = {
        "input":   tmp_path / "dim_location.parquet",
        "output":  tmp_path / "dim_location_enriched.parquet",
        "city_db": tmp_path / "GeoLite2-City.mmdb",
        "asn_db":  tmp_path / "GeoLite2-ASN.mmdb",
    }
    mocker.patch("src.localization.DIM_LOCATION_FILE",          paths["input"])
    mocker.patch("src.localization.DIM_LOCATION_ENRICHED_FILE", paths["output"])
    mocker.patch("src.localization.CITY_DB",                    paths["city_db"])
    mocker.patch("src.localization.ASN_DB",                     paths["asn_db"])
    return paths


@pytest.fixture()
def patch_reader(mocker, mock_geoip2_reader):
    """
    Parchea geoip2.database.Reader como context manager.
    Los .mmdb no necesitan existir físicamente.
    Devuelve mock_geoip2_reader para que los tests puedan configurar
    side_effects o assert sobre llamadas.
    """
    def _make_ctx(_path):
        ctx = MagicMock()
        ctx.__enter__.return_value = mock_geoip2_reader
        ctx.__exit__.return_value = False
        return ctx

    mocker.patch("geoip2.database.Reader", side_effect=_make_ctx)
    return mock_geoip2_reader


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _public_ip_df(*ips, countries=None, ids=None):
    """DataFrame mínimo con IPs públicas RFC 5737 (nunca resueltas por GeoIP2 real)."""
    n = len(ips)
    return pd.DataFrame({
        "ip_addr":      list(ips),
        "conn_country": countries or ["US"] * n,
        "location_id":  ids or list(range(n)),
    })


def _mock_city_response(city="Madrid", country="Spain", region="Madrid",
                        lat=40.4168, lon=-3.7038):
    """
    Construye un MagicMock de respuesta city() con atributos string reales.
    Necesario para que PyArrow pueda serializar el DataFrame a parquet
    sin encontrar MagicMocks no tipados en las columnas.
    """
    r = MagicMock()
    r.city.name = city
    r.country.name = country          # accedido como res_city.country.name en el módulo
    r.subdivisions.most_specific.name = region
    r.location.latitude = lat
    r.location.longitude = lon
    return r


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSkipGeolocationIp:
    """Ramas de _skip_geolocation_ip (IP inválida, IPv6)."""

    def test_invalid_string_skipped(self):
        assert _skip_geolocation_ip("not-an-ip-address") is True

    def test_ipv6_loopback_skipped(self):
        assert _skip_geolocation_ip("::1") is True

    def test_public_ipv6_not_skipped(self):
        # Rama IPv6 distinta de RFC1918-only IPv4; dirección pública genérica.
        assert _skip_geolocation_ip("2606:4700:4700::1111") is False


class TestEnrichLocationDimension:

    def test_creates_enriched_output_file(self, patch_paths, patch_reader):
        """Crea dim_location_enriched.parquet con todas las columnas esperadas."""
        _public_ip_df("203.0.113.1").to_parquet(patch_paths["input"])

        enrich_location_dimension()

        assert patch_paths["output"].exists()
        enriched = pd.read_parquet(patch_paths["output"])
        assert len(enriched) > 0
        for col in ("city", "region", "isp", "country", "latitude", "longitude"):
            assert col in enriched.columns, f"Falta columna '{col}'"

    def test_preserves_original_columns(self, patch_paths, patch_reader):
        """Las columnas originales (ip_addr, conn_country, location_id) no se modifican."""
        _public_ip_df("203.0.113.1", countries=["ES"], ids=[42]).to_parquet(patch_paths["input"])

        enrich_location_dimension()

        row = pd.read_parquet(patch_paths["output"]).iloc[0]
        assert row["ip_addr"]      == "203.0.113.1"
        assert row["conn_country"] == "ES"
        assert row["location_id"]  == 42

    def test_filters_unknown_and_private_ips(self, patch_paths, patch_reader):
        """0.0.0.0 y rangos privados RFC 1918 no generan llamadas a GeoIP2."""
        pd.DataFrame({
            "ip_addr":      ["0.0.0.0", "192.168.1.1", "10.0.0.1"],
            "conn_country": ["XX",       "ES",           "PRIVATE"],
            "location_id":  [-1,          0,              1],
        }).to_parquet(patch_paths["input"])

        enrich_location_dimension()

        assert patch_reader.city.call_count == 0

    def test_fills_unknown_values_on_lookup_error(self, patch_paths, patch_reader):
        """
        Cuando GeoIP2 falla en una IP, esa fila se rellena con 'Unknown' y coordenadas 0.0.

        asn() solo se invoca si city() no lanza; un side_effect en asn con la misma
        longitud que city desincroniza las respuestas.
        """
        patch_reader.city.side_effect = [
            geoip2.errors.GeoIP2Error("Lookup failed"),
            _mock_city_response("London", "United Kingdom", "Greater London", 51.5, -0.1),
        ]
        patch_reader.asn.return_value = MagicMock(autonomous_system_organization="BT")
        _public_ip_df("203.0.113.1", "198.51.100.2").to_parquet(patch_paths["input"])

        enrich_location_dimension()

        failed = pd.read_parquet(patch_paths["output"])
        failed = failed[failed["ip_addr"] == "203.0.113.1"].iloc[0]
        assert failed["city"]      == "Unknown"
        assert failed["latitude"]  == 0.0
        assert failed["longitude"] == 0.0

    def test_continues_after_partial_lookup_failure(self, patch_paths, patch_reader):
        """Si una IP falla, las demás se procesan y el resultado contiene todas las filas."""
        patch_reader.city.side_effect = [
            geoip2.errors.GeoIP2Error("fail"),
            _mock_city_response("London", "United Kingdom", "Greater London", 51.5, -0.1),
        ]
        patch_reader.asn.return_value = MagicMock(autonomous_system_organization="BT")
        _public_ip_df("203.0.113.1", "198.51.100.2", countries=["US", "UK"]).to_parquet(
            patch_paths["input"]
        )

        enrich_location_dimension()

        enriched = pd.read_parquet(patch_paths["output"])
        assert len(enriched) == 2
        assert patch_reader.city.call_count == 2

    def test_returns_correct_enriched_values(self, patch_paths, patch_reader):
        """Los valores devueltos por GeoIP2 se mapean correctamente en la salida."""
        patch_reader.city.return_value = _mock_city_response(
            "New York", "United States", "New York", 40.7128, -74.006
        )
        _public_ip_df("203.0.113.1").to_parquet(patch_paths["input"])

        enrich_location_dimension()

        enriched = pd.read_parquet(patch_paths["output"])
        row = enriched.iloc[0]
        assert row["city"]      == "New York"
        assert row["country"]   == "United States"
        assert row["latitude"]  == 40.7128
        assert row["longitude"] == -74.006
        assert pd.api.types.is_numeric_dtype(enriched["latitude"])
        assert pd.api.types.is_numeric_dtype(enriched["longitude"])

    def test_missing_input_file_raises_and_logs_error(self, patch_paths, mocker):
        """Si no existe dim_location.parquet, lanza FileNotFoundError y loguea el error."""
        mock_logger = mocker.patch("src.localization.logger")
        # patch_paths["input"] no existe — no se creó

        with pytest.raises(FileNotFoundError):
            enrich_location_dimension()

        assert mock_logger.error.called
        logged_msg = str(mock_logger.error.call_args)
        assert any(kw in logged_msg.lower() for kw in ("dim_location", "no se encontró", "not found"))

    def test_missing_databases_raises_and_logs_error(self, patch_paths, mocker):
        """Si geoip2.database.Reader no puede abrir los .mmdb, se relanza el error y se loguea."""
        mock_logger = mocker.patch("src.localization.logger")
        _public_ip_df("203.0.113.1").to_parquet(patch_paths["input"])

        mocker.patch(
            "geoip2.database.Reader",
            side_effect=FileNotFoundError("GeoLite2-City.mmdb not found"),
        )

        with pytest.raises(FileNotFoundError):
            enrich_location_dimension()

        assert mock_logger.error.called

    def test_to_parquet_failure_raises_and_logs(self, patch_paths, patch_reader, mocker):
        """Si falla la escritura del parquet enriquecido, se propaga y se loguea."""
        mock_logger = mocker.patch("src.localization.logger")
        _public_ip_df("203.0.113.1").to_parquet(patch_paths["input"])
        mocker.patch(
            "pandas.DataFrame.to_parquet",
            side_effect=OSError("simulated write failure"),
        )

        with pytest.raises(OSError, match="simulated write failure"):
            enrich_location_dimension()

        assert mock_logger.error.called

    def test_main_cli_logs_exception(self, mocker):
        """Punto de entrada CLI ante error en enrich_location_dimension."""
        mocker.patch(
            "src.localization.enrich_location_dimension",
            side_effect=RuntimeError("cli fail"),
        )
        mock_log = mocker.patch("src.localization.logger")
        _main_cli()
        mock_log.error.assert_called()