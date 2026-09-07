# Portafolio Spotify

Pipeline ETL en Python para transformar historiales de reproducción de Spotify
(JSON) en un modelo dimensional en formato Parquet, listo para análisis en
Power BI u otras herramientas de BI.

## Qué hace

- Extrae y concatena historiales desde `data/raw/`.
- Limpia registros y genera dimensiones de fecha, dispositivo, track, episodio y ubicación.
- Construye `fact_table.parquet` con claves foráneas y métricas de reproducción.
- Enriquece IP públicas con GeoLite2 City y ASN cuando las bases están disponibles.
- Registra el proceso en `logs/`.

## Requisitos y ejecución

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python main.py
```

Coloca los JSON de Spotify en `data/raw/`. Para el enriquecimiento geográfico,
coloca `GeoLite2-City.mmdb` y `GeoLite2-ASN.mmdb` en `data/Databases/`. La
geolocalización es opcional; el ETL principal puede completarse sin esas bases.

Para procesar otro conjunto, define `SPOTIFY_USER_SUFFIX`. Por ejemplo,
`SPOTIFY_USER_SUFFIX=Pedro` utiliza `data/rawPedro/` y `data/processedPedro/`.
Las bases GeoLite2 se leen siempre desde `data/Databases/`, independientemente
del sufijo.

## Salidas

El proceso escribe en `data/processed/` (o en la ruta correspondiente al sufijo):

```text
dim_date.parquet
dim_device.parquet
dim_track.parquet
dim_episode.parquet
dim_location.parquet
fact_table.parquet
dim_location_enriched.parquet  # si la geolocalización se completa
```

## Tests

```powershell
pytest
pytest --cov=src --cov-report=html
```

La suite incluye pruebas unitarias y de integración.

## Estructura

```text
src/                    # ETL, geolocalización, validación y utilidades
tests/                  # Suite de pytest
data/raw/               # Entrada local, no versionada
data/processed/         # Salidas Parquet, no versionadas
data/Databases/         # Bases GeoLite2, no versionadas
main.py                 # Orquestador
config.py               # Rutas y constantes
TECHNICAL_DOCUMENTATION.md
```

Los datos, bases GeoLite2, logs, reportes de cobertura  y
los scripts de análisis local no se versionan.
