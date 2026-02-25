"""
Configuración centralizada del proyecto.
Define rutas y constantes globales usadas por todo el pipeline.
"""

from pathlib import Path
import os

# ============================================
# RUTAS BASE
# ============================================
PROJECT_ROOT = Path(__file__).parent
SRC_DIR = PROJECT_ROOT / "src"
DATA_DIR = PROJECT_ROOT / "data"

# Variables de entorno para permitir multi-usuario (flexible)
# Si no está definida, usa "raw" y "processed" estándar
USER_SUFFIX = os.getenv("SPOTIFY_USER_SUFFIX", "")
RAW_DATA_DIR = DATA_DIR / f"raw{USER_SUFFIX}"
PROCESSED_DATA_DIR = DATA_DIR / f"processed{USER_SUFFIX}"

LOGS_DIR = PROJECT_ROOT / "logs"
DB_DIR = DATA_DIR / f"Databases{USER_SUFFIX}"

# Crear directorios si no existen
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
DB_DIR.mkdir(parents=True, exist_ok=True)

# ============================================
# ARCHIVOS DE BASES DE DATOS (GeoLite2)
# ============================================
CITY_DB = DB_DIR / "GeoLite2-City.mmdb"
ASN_DB = DB_DIR / "GeoLite2-ASN.mmdb"
COUNTRY_DB = DB_DIR / "GeoLite2-Country.mmdb"

# ============================================
# ARCHIVOS PROCESADOS (PARQUET)
# ============================================
DIM_DATE_FILE = PROCESSED_DATA_DIR / "dim_date.parquet"
DIM_DEVICE_FILE = PROCESSED_DATA_DIR / "dim_device.parquet"
DIM_TRACK_FILE = PROCESSED_DATA_DIR / "dim_track.parquet"
DIM_EPISODE_FILE = PROCESSED_DATA_DIR / "dim_episode.parquet"
DIM_LOCATION_FILE = PROCESSED_DATA_DIR / "dim_location.parquet"
DIM_LOCATION_ENRICHED_FILE = PROCESSED_DATA_DIR / "dim_location_enriched.parquet"
FACT_TABLE_FILE = PROCESSED_DATA_DIR / "fact_table.parquet"

# ============================================
# CONFIGURACIÓN DE LOGGING
# ============================================
LOG_LEVEL = "DEBUG"
LOG_FORMAT = '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# ============================================
# CONSTANTES DE DATOS
# ============================================
UNKNOWN_VALUE = "Unknown"
UNKNOWN_ID = -1
UNKNOWN_COORDS = (0.0, 0.0)

# Columnas a ignorar en limpieza
AUDIOBOOK_COLUMNS_PATTERN = "audiobook"
