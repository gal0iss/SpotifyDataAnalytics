"""
Configuración y fixtures compartidas para los tests.

Proporciona datos de prueba, mocks y funciones auxiliares para la suite de tests.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# Datos de prueba
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_streaming_data():
    """DataFrame pequeño con datos de streaming de Spotify."""
    return pd.DataFrame({
        "ts": pd.to_datetime([
            "2024-01-01 10:30:00",
            "2024-01-01 11:00:00",
            "2024-01-02 09:15:00",
        ]),
        "platform": ["Windows", "Android", "MacOS"],
        "master_metadata_track_name": ["Track A", "Track B", "Track C"],
        "master_metadata_album_artist_name": ["Artist 1", "Artist 2", "Artist 3"],
        "master_metadata_album_album_name": ["Album 1", "Album 2", "Album 3"],
        "spotify_track_uri": ["uri:track:001", "uri:track:002", "uri:track:003"],
        "spotify_episode_uri": [None, None, None],
        "episode_name": [None, None, None],
        "episode_show_name": [None, None, None],
        # IPs públicas RFC 5737 — nunca se resuelven a ubicaciones reales
        "ip_addr": ["203.0.113.1", "203.0.113.2", "203.0.113.3"],
        "conn_country": ["ES", "US", "UK"],
        "ms_played": [180000, 240000, 150000],
        "skipped": [False, True, False],
        "shuffle": [True, False, False],
        "offline": [False, False, False],
        "incognito_mode": [False, False, False],
    })


@pytest.fixture
def sample_streaming_data_with_podcasts():
    """DataFrame con tracks y episodios de podcast mezclados."""
    return pd.DataFrame({
        "ts": pd.to_datetime(["2024-01-01 10:00:00", "2024-01-01 11:00:00"]),
        "platform": ["iOS", "Web"],
        "master_metadata_track_name": ["Song", "Non-Music"],
        "master_metadata_album_artist_name": ["Artist", "N/A"],
        "master_metadata_album_album_name": ["Album", "N/A"],
        "spotify_track_uri": ["uri:track:100", None],
        "spotify_episode_uri": [None, "uri:episode:001"],
        "episode_name": [None, "Episode Title"],
        "episode_show_name": [None, "Show Name"],
        "ip_addr": ["203.0.113.10", "198.51.100.1"],
        "conn_country": ["AU", "CA"],
        "ms_played": [200000, 300000],
        "skipped": [False, False],
        "shuffle": [False, False],
        "offline": [False, False],
        "incognito_mode": [False, True],
    })


@pytest.fixture
def sample_dim_location():
    """
    Tabla dimensional de ubicaciones con IPs públicas (RFC 5737).
    Usar IPs públicas garantiza que los tests de enriquecimiento lleguen
    al bloque de consulta de GeoIP2 sin ser filtradas antes.
    """
    return pd.DataFrame({
        "ip_addr": ["203.0.113.1", "203.0.113.2", "198.51.100.1", "0.0.0.0"],
        "conn_country": ["ES", "US", "UK", "XX"],
        "location_id": [0, 1, 2, -1],
    })


# ---------------------------------------------------------------------------
# Infraestructura de directorios temporales
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_data_dir(tmp_path):
    """Estructura temporal de directorios que imita el layout real del proyecto."""
    dirs = {
        "root":      tmp_path,
        "raw":       tmp_path / "data" / "raw",
        "processed": tmp_path / "data" / "processed",
        "databases": tmp_path / "data" / "databases",
    }
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    return dirs


# ---------------------------------------------------------------------------
# Mocks de GeoIP2
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_geoip2_reader():
    """
    Reader de GeoIP2 pre-configurado con respuestas string reales.

    Configura explícitamente todos los atributos que accede el módulo
    (city, country, subdivisions, location, asn) para evitar que
    MagicMock auto-genere objetos no serializables por PyArrow.
    """
    reader = MagicMock()

    city_response = MagicMock()
    city_response.city.name = "Madrid"
    city_response.country.name = "Spain"           # <-- accedido como res_city.country.name
    city_response.subdivisions.most_specific.name = "Community of Madrid"
    city_response.location.latitude = 40.4168
    city_response.location.longitude = -3.7038
    reader.city.return_value = city_response

    asn_response = MagicMock()
    asn_response.autonomous_system_organization = "Telefonica"
    reader.asn.return_value = asn_response

    return reader