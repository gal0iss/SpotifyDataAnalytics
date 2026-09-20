# Documentación Técnica — Portafolio Spotify
 
**Pipeline ETL con Modelado Dimensional (Star Schema)**
 
| | |
|---|---|
| **Versión** | 2.0 (consolidada) |
| **Última revisión** | Septiembre 2026 |
| **Autor** | Pedro Parada |
| **Rol** | Data Engineer / Backend Developer |
| **Estado** | Documentación técnica — Portafolio público |
 
> **Nota sobre esta versión:** este documento reconcilia dos documentaciones previas del mismo proyecto que presentaban algunas diferencias entre sí. Los puntos donde las fuentes originales no coincidían se marcan con ⚠️ y se listan también en la [sección 19](#19-notas-de-reconciliación-puntos-a-verificar) para su confirmación contra el código fuente.
 
---
 
## Tabla de contenidos
 
1. [Descripción general](#1-descripción-general)
2. [Alcance](#2-alcance)
3. [Stack tecnológico](#3-stack-tecnológico)
4. [Arquitectura del sistema](#4-arquitectura-del-sistema)
5. [Estructura de directorios](#5-estructura-de-directorios)
6. [Configuración (`config.py`)](#6-configuración-configpy)
7. [Orquestación (`main.py`)](#7-orquestación-mainpy)
8. [Modelo de datos — Star Schema](#8-modelo-de-datos--star-schema)
9. [ETL Core (`src/ReadProcess.py`)](#9-etl-core-srcreadprocesspy)
10. [Geolocalización (`src/localization.py`)](#10-geolocalización-srclocalizationpy)
11. [Validación de calidad (`src/validate.py`)](#11-validación-de-calidad-srcvalidatepy)
12. [Logging (`src/logger.py`)](#12-logging-srcloggerpy)
13. [Utilidades (`src/utils.py`)](#13-utilidades-srcutilspy)
14. [Artefactos generados](#14-artefactos-generados)
15. [Patrones de diseño](#15-patrones-de-diseño)
16. [Convenciones de código](#16-convenciones-de-código)
17. [Instalación y ejecución](#17-instalación-y-ejecución)
18. [Suite de tests](#18-suite-de-tests)
19. [Notas de reconciliación (puntos a verificar)](#19-notas-de-reconciliación-puntos-a-verificar)
20. [Resumen técnico](#20-resumen-técnico)
---
 
## 1. Descripción general
 
**Portafolio Spotify** es un pipeline ETL (Extract, Transform, Load) que transforma el historial de reproducción exportado por Spotify (archivos JSON) en un **modelo dimensional en esquema estrella (Star Schema)**, persistido en formato **Apache Parquet**.
 
La arquitectura está orientada a consultas analíticas y Business Intelligence, permitiendo conectar los datos resultantes con herramientas como Power BI.
 
El sistema incorpora:
 
- Procesamiento ETL modular (Extract, Clean, Transform, Load).
- Modelado dimensional con claves subrogadas (*surrogate keys*).
- Persistencia en Apache Parquet.
- Enriquecimiento geográfico mediante MaxMind GeoLite2.
- Validación de calidad de datos post-procesamiento.
- Logging estructurado con salida dual (consola + archivo).
- Soporte multiusuario mediante variable de entorno.
- Manejo diferenciado de errores (fatales vs. recuperables).
- Suite de tests con pytest.
---
 
## 2. Alcance
 
El proyecto cubre el ciclo completo desde la ingesta de archivos JSON de Spotify hasta la generación de tablas Parquet listas para análisis, más un enriquecimiento geográfico opcional y un reporte de calidad sobre ese enriquecimiento.
 
El orquestador (`main.py`) ejecuta el pipeline en fases secuenciales — ver [sección 7](#7-orquestación-mainpy) para el detalle y la nota de reconciliación sobre el número exacto de fases.
 
---
 
## 3. Stack tecnológico
 
| Tecnología | Uso |
|---|---|
| Python 3.x | Lenguaje principal |
| pandas | Manipulación y transformación de datos |
| PyArrow | Serialización y lectura/escritura Parquet |
| GeoIP2 | Integración con MaxMind GeoLite2 |
| pytest | Testing |
| pytest-mock | Mocking |
| pytest-cov | Cobertura de tests |
| python-dotenv | Variables de entorno |
| Apache Parquet | Persistencia de datos |
 
---
 
## 4. Arquitectura del sistema
 
```text
data/raw/*.json
      │
      ▼
src.ReadProcess  (Extract → Clean → Transform → Load)
      │
      ├──► dim_date.parquet
      ├──► dim_device.parquet
      ├──► dim_track.parquet
      ├──► dim_episode.parquet
      ├──► dim_location.parquet
      └──► fact_table.parquet
      │
      ▼
src.localization + GeoLite2 (City + ASN)
      │
      ▼
dim_location_enriched.parquet
      │
      ▼
src.validate  (reporte de calidad, no bloqueante)
      │
      ▼
Power BI / herramientas de BI
```
 
La tabla de hechos usa `event_id` como clave primaria y referencia a las dimensiones mediante `date_id`, `device_id`, `track_id`, `episode_id` y `location_id`. Los valores sin correspondencia se representan con `UNKNOWN_ID = -1`, lo que preserva la integridad referencial incluso ante datos faltantes.
 
---
 
## 5. Estructura de directorios
 
```text
spotify-data-analytics/
│
├── src/
│   ├── ReadProcess.py
│   ├── localization.py
│   ├── validate.py
│   ├── logger.py
│   └── utils.py
│
├── tests/
│   ├── test_read_process.py
│   ├── test_localization.py
│   ├── test_pipeline.py
│   ├── test_utils.py
│   ├── test_validate.py
│   └── conftest.py
│
├── data/
│   ├── raw{SUFFIX}/
│   ├── processed{SUFFIX}/
│   └── Databases/
│       ├── GeoLite2-City.mmdb
│       └── GeoLite2-ASN.mmdb
│
├── logs/
│   └── pipeline_YYYYMMDD_HHMMSS.log
│
├── docs/
│   └── images/
│
├── config.py
├── main.py
├── requirements.txt
└── pytest.ini
```
 
| Ruta | Contenido |
|---|---|
| `src/` | Módulos ETL, geolocalización, validación, logging y utilidades |
| `tests/` | Tests unitarios, de integración, fixtures y mocks |
| `data/raw{SUFFIX}/` | JSONs de entrada exportados desde Spotify |
| `data/processed{SUFFIX}/` | Archivos Parquet generados |
| `data/Databases/` | Bases MaxMind GeoLite2 (siempre globales, no se versionan por usuario) |
| `logs/` | Archivos de log generados en runtime |
| `docs/images/` | Capturas y visualizaciones de Power BI |
| `config.py` | Configuración centralizada |
| `main.py` | Punto de entrada y orquestación |
 
Los directorios de datos y logs no están versionados; se crean durante la ejecución.
 
---
 
## 6. Configuración (`config.py`)
 
`config.py` es la fuente única de configuración del pipeline. Define rutas de entrada/salida, rutas de las bases GeoLite2, constantes de dominio, parámetros de logging e identificadores para valores desconocidos.
 
Rutas por defecto:
 
```text
data/raw/          Entrada JSON
data/processed/    Salida Parquet
data/Databases/    GeoLite2 City y ASN
logs/              Logs con marca temporal
```
 
Durante la importación se crean los directorios requeridos mediante `mkdir(parents=True, exist_ok=True)`, lo que introduce efectos secundarios al importar el módulo (por eso las rutas se parchean en ciertos tests).
 
**Soporte multiusuario:** la variable `SPOTIFY_USER_SUFFIX` permite separar datasets por usuario. Por ejemplo, con el sufijo `Pedro`, las rutas se resuelven como `data/rawPedro/` y `data/processedPedro/`. Las bases GeoLite2 **no** se ven afectadas por el sufijo: siempre se leen desde la ruta global `data/Databases/`.
 
### Principales constantes
 
| Constante / Variable | Propósito |
|---|---|
| `USER_SUFFIX` | Sufijo obtenido desde `SPOTIFY_USER_SUFFIX` |
| `RAW_DATA_DIR` | Ruta de datos de entrada |
| `PROCESSED_DATA_DIR` | Ruta de datos procesados |
| `CITY_DB` | Ruta a `GeoLite2-City.mmdb` |
| `ASN_DB` | Ruta a `GeoLite2-ASN.mmdb` |
| `DIM_*_FILE` | Rutas de salida de cada dimensión |
| `FACT_TABLE_FILE` | Ruta de salida de la tabla de hechos |
| `UNKNOWN_VALUE` | Valor textual para información desconocida |
| `UNKNOWN_ID` | Surrogate key para valores desconocidos (`-1`) |
| `AUDIOBOOK_COLUMNS_PATTERN` | Patrón para detectar columnas de audiobook completamente nulas |
 
---
 
## 7. Orquestación (`main.py`)
 
`main.py` es el punto de entrada del sistema vía CLI. Ejecuta el pipeline en fases secuenciales con manejo de errores diferenciado entre etapas fatales y no fatales. ⚠️ *Ver nota de reconciliación sobre el número exacto de fases documentadas.*
 
```text
┌─────────────────────┐
│ 1. Pre-validación    │   src/utils.py — validación de rutas/directorios
└──────────┬───────────┘
           ▼
┌─────────────────────┐
│ 2. ETL               │   src/ReadProcess.py — extracción, limpieza,
└──────────┬───────────┘   dimensiones y tabla de hechos
           ▼
┌─────────────────────┐
│ 3. Geolocalización    │   src/localization.py — enriquecimiento opcional
└──────────┬───────────┘   con GeoLite2
           ▼
┌─────────────────────┐
│ 4. Validación        │   src/validate.py — reporte de calidad
└─────────────────────┘
```
 
### Manejo de errores por fase
 
| Fase | Módulo | Comportamiento ante fallo |
|---|---|---|
| Pre-validación | `src/utils.py` | Fatal — `sys.exit(1)` |
| ETL | `src/ReadProcess.py` | Fatal — `sys.exit(1)` |
| Geolocalización | `src/localization.py` | No fatal — se registra advertencia y el pipeline continúa con las salidas ETL |
| Validación | `src/validate.py` | No fatal — se registra advertencia; es un reporte de calidad, no sustituye pruebas automatizadas ni detiene el pipeline |
 
Esta diferenciación separa los errores que impiden generar el dataset (rutas inválidas, fallos del ETL) de los errores parciales que solo afectan un enriquecimiento o una estadística (GeoLite2 ausente, una métrica del reporte fallando).
 
---
 
## 8. Modelo de datos — Star Schema
 
El modelo utiliza un esquema estrella: una tabla de hechos central y cinco dimensiones, todas con **surrogate keys** enteras y secuenciales. Los valores desconocidos usan `UNKNOWN_ID = -1`.
 
```text
                         ┌──────────────┐
                         │  dim_date    │
                         └──────┬───────┘
                                │
┌──────────────┐          ┌────▼─────────────┐          ┌──────────────┐
│ dim_device   │─────────►│   fact_table     │◄─────────│  dim_track   │
└──────────────┘          └────┬─────────────┘          └──────────────┘
                                │
                  ┌─────────────┴─────────────┐
                  │                            │
          ┌───────▼────────┐          ┌───────▼─────────┐
          │  dim_episode   │          │  dim_location    │
          └────────────────┘          └──────────────────┘
```
 
### 8.1 `fact_table`
 
| Campo | Descripción |
|---|---|
| `event_id` | Primary key del evento |
| `date_id` | Foreign key hacia `dim_date` |
| `track_id` | Foreign key hacia `dim_track` |
| `episode_id` | Foreign key hacia `dim_episode` |
| `device_id` | Foreign key hacia `dim_device` |
| `location_id` | Foreign key hacia `dim_location` |
| `ms_played` | Milisegundos reproducidos |
| `skipped` | Indica si el contenido fue omitido |
| `shuffle` | Indica reproducción aleatoria |
| `offline` | Indica reproducción offline |
| `incognito_mode` | Indica modo incógnito |
 
Las foreign keys sin correspondencia dimensional reciben `UNKNOWN_ID`. Los campos booleanos (`skipped`, `shuffle`, `offline`, `incognito_mode`) se convierten explícitamente a `bool` para compatibilidad con PyArrow, Parquet y herramientas de BI.
 
### 8.2 Dimensiones
 
| Dimensión | Clave | Clave natural | Contenido |
|---|---|---|---|
| `dim_date` | `date_id` | `YYYYMMDDHH` | Año, mes, día, día de semana y hora (granularidad horaria) |
| `dim_device` | `device_id` | `platform` | Plataforma y tipo de dispositivo |
| `dim_track` | `track_id` | `spotify_track_uri` | URI, canción, artista y álbum |
| `dim_episode` | `episode_id` | `spotify_episode_uri` | URI, episodio y programa |
| `dim_location` | `location_id` | `ip_addr` | IP y país reportado por Spotify (`conn_country`); tras el enriquecimiento suma `country`, `city`, `region`, `isp`, `latitude`, `longitude` |
 
⚠️ **`dim_device` — categorías de clasificación:** las fuentes originales no coinciden en si `classify_device()` produce 4 categorías (`mobile`, `desktop`, `web`, `other`) o 5 (agregando `unknown`). Ver [sección 19](#19-notas-de-reconciliación-puntos-a-verificar).
 
Para `dim_track` y `dim_episode`, se eliminan primero los registros cuya URI sea nula. Se usa `pd.StringDtype()` para mantener consistencia de tipos durante los merges.
 
---
 
## 9. ETL Core (`src/ReadProcess.py`)
 
Módulo principal del pipeline (~400 líneas). Implementa las cuatro fases: **Extract → Clean → Transform → Load**.
 
### 9.1 Extract — `extract_json_files(path)`
 
Lee todos los archivos `*.json` del directorio configurado, convierte la columna `ts` a fecha (`convert_dates=["ts"]`) y concatena los DataFrames resultantes.
 
La operación falla si la ruta no existe o no contiene archivos JSON con entradas válidas, lanzando `FileNotFoundError`. ⚠️ *Confirmar si "entradas válidas" implica una validación adicional del contenido más allá de la simple existencia de archivos `.json` — ver sección 19.*
 
### 9.2 Clean — `clean_data(df)`
 
1. Elimina columnas relacionadas con `audiobook` cuando todas sus celdas son nulas.
2. Reinicia el índice.
3. Asigna `event_id` secuencial (`0, 1, 2, ..., N-1`) como clave primaria de la futura tabla de hechos.
### 9.3 Transform — `create_dim_*()`
 
Cada función de dimensión sigue un patrón común:
 
```text
Selección de atributos
        │
        ▼
Deduplicación
        │
        ▼
Identificación por clave natural
        │
        ▼
Asignación de surrogate key
        │
        ▼
Inserción de fila "Unknown" (-1)
```
 
### 9.4 Load — `create_fact_table(df)`
 
1. Recalcula `date_id` a partir del timestamp.
2. Realiza `left merge` con cada dimensión, usando solo clave natural y surrogate key (para no inflar columnas).
3. Reemplaza foreign keys nulas por `UNKNOWN_ID`.
4. Convierte los campos booleanos (`skipped`, `shuffle`, `offline`, `incognito_mode`) a `bool`.
`save_to_parquet()` persiste cada DataFrame resultante (dimensiones y tabla de hechos) en la ruta de salida configurada, con `index=False`.
 
---
 
## 10. Geolocalización (`src/localization.py`)
 
`enrich_location_dimension()` lee `dim_location.parquet` y consulta las IP públicas únicas contra:
 
- **`GeoLite2-City.mmdb`** → ciudad, país, región y coordenadas.
- **`GeoLite2-ASN.mmdb`** → organización autónoma / ISP.
### 10.1 Filtrado de IPs
 
Se omiten (no se consultan contra GeoLite2):
 
| Tipo de IP | Acción |
|---|---|
| RFC 1918 (`10.x`, `172.16.x`, `192.168.x`) | Omitir |
| Loopback (IPv4/IPv6) | Omitir |
| Link-local (IPv4/IPv6) | Omitir |
| Multicast | Omitir |
| Unspecified | Omitir |
| IPv6 privadas | Omitir |
| `0.0.0.0` | Omitir |
| Valores `unknown` / `Unknown` | Omitir |
| IP pública válida | Consultar GeoLite2 |
 
### 10.2 Enriquecimiento
 
El resultado de las consultas se combina mediante `left merge` con la dimensión de ubicación original. Se preserva `conn_country` (reportado por Spotify) y se agregan `country`, `city`, `region`, `isp`, `latitude`, `longitude`.
 
Las IPs sin coincidencia en GeoLite2 conservan valores `Unknown` en los campos de texto y coordenadas `(0.0, 0.0)`.
 
El resultado se guarda en `dim_location_enriched.parquet`. Si faltan las bases GeoLite2, `main.py` registra una advertencia y continúa con las salidas del ETL principal (comportamiento no fatal, ver [sección 7](#7-orquestación-mainpy)).
 
---
 
## 11. Validación de calidad (`src/validate.py`)
 
`validate_data()` inspecciona `dim_location_enriched.parquet` y reporta:
 
- Número total de registros.
- Cobertura de información geográfica (proporción de valores distintos de `Unknown`).
- Top 10 ciudades.
- Top 10 ISPs.
Cada métrica está protegida por un `try/except` independiente, de modo que un fallo en una métrica no impide obtener el resto del reporte. Es un reporte de calidad — no sustituye a las pruebas automatizadas ni detiene el pipeline por una estadística incompleta.
 
---
 
## 12. Logging (`src/logger.py`)
 
Logger singleton (`spotify_pipeline`) con dos handlers:
 
- **`StreamHandler`** → salida a `stdout`, para seguimiento en terminal.
- **`FileHandler`** → archivo `logs/pipeline_YYYYMMDD_HHMMSS.log`, codificación UTF-8.
**Formato:**
 
```text
YYYY-MM-DD HH:MM:SS | LEVEL | módulo | mensaje
```
 
Si el archivo de log no puede crearse, el sistema continúa funcionando solo con salida por consola. Los errores del ETL principal terminan la ejecución; los errores de geolocalización y del reporte de validación se registran como advertencias cuando son recuperables.
 
---
 
## 13. Utilidades (`src/utils.py`)
 
| Función | Contrato |
|---|---|
| `validate_input_file(path, file_type)` | Verifica existencia y que el path corresponda a un archivo. Retorna `bool`. |
| `validate_input_directory(path, must_contain)` | Verifica existencia del directorio y, opcionalmente, que contenga archivos que coincidan con un patrón `glob`. |
| `ensure_output_directory(output_dir)` | Crea el directorio con `mkdir(parents=True, exist_ok=True)`. Captura excepciones y retorna `bool`. |
 
---
 
## 14. Artefactos generados
 
El pipeline produce **siete archivos Parquet** en `data/processed{SUFFIX}/`, todos con `index=False`, compatibles con conectores de BI.
 
| Archivo | Módulo | Descripción |
|---|---|---|
| `dim_date.parquet` | `ReadProcess` | Dimensión temporal con granularidad horaria (`YYYYMMDDHH`) |
| `dim_device.parquet` | `ReadProcess` | Clasificación de plataformas ⚠️ (ver sección 19) |
| `dim_track.parquet` | `ReadProcess` | Catálogo de canciones, artistas y álbumes |
| `dim_episode.parquet` | `ReadProcess` | Catálogo de episodios/podcasts |
| `dim_location.parquet` | `ReadProcess` | IPs únicas y código ISO de país proporcionado por Spotify |
| `fact_table.parquet` | `ReadProcess` | Tabla de hechos con FKs, duración y flags |
| `dim_location_enriched.parquet` | `localization` | Dimensión geográfica enriquecida mediante MaxMind |
 
---
 
## 15. Patrones de diseño
 
| Patrón | Aplicación |
|---|---|
| **Star Schema** | Tabla de hechos central conectada con dimensiones normalizadas, optimizada para consultas analíticas |
| **ETL Pipeline** | Separación explícita de Extract, Transform y Load |
| **Singleton** | Logger global y configuración centralizada |
| **Factory** | Funciones `create_dim_*()` comparten un contrato uniforme para construir dimensiones |
| **Strategy** | `classify_device()` encapsula la lógica de clasificación de plataformas |
 
---
 
## 16. Convenciones de código
 
- **Funciones y variables:** `snake_case`
- **Constantes:** `UPPER_SNAKE_CASE`
- **Funciones privadas:** prefijo `_` (ej. `_skip_geolocation_ip()`)
- **Type hints** en parámetros y retornos, ej.: `def classify_device(platform: str) -> str: ...`
- **Docstrings** estilo Google/NumPy, incluyendo `Args`, `Returns`, `Raises`
- **Manejo de errores:** excepciones específicas para errores no críticos; se usa `exc_info=True` al loguear para preservar el stack trace completo
---
 
## 17. Instalación y ejecución
 
### 17.1 Requisitos
 
- Python 3.x
- Dependencias declaradas en `requirements.txt`: `pandas`, `numpy`, `pyarrow`, `geoip2`, `pytest`, `pytest-mock`, `pytest-cov`, `python-dotenv`
- Para el enriquecimiento geográfico: `GeoLite2-City.mmdb` y `GeoLite2-ASN.mmdb` (bases de MaxMind, no incluidas en el repositorio)
### 17.2 Instalación
 
```bash
pip install -r requirements.txt
```
 
### 17.3 Ejecución completa
 
**Usuario por defecto** (usa `data/raw` y `data/processed`):
 
```bash
python main.py
```
 
**Usuario alternativo** (usa `SPOTIFY_USER_SUFFIX` para separar datasets):
 
```bash
SPOTIFY_USER_SUFFIX=Pedro python main.py
```
 
Esto resuelve las rutas como `data/rawPedro` y `data/processedPedro`. Las bases GeoLite2 siguen siendo globales.
 
### 17.4 Ejecución por fases
 
```bash
# Solo ETL
python -m src.ReadProcess
 
# Solo enriquecimiento geográfico
python -m src.localization
 
# Solo validación
python -m src.validate
```
 
---
 
## 18. Suite de tests
 
Implementada con **pytest**.
 
### 18.1 Organización
 
| Archivo | Módulo testeado | Tipo |
|---|---|---|
| `test_read_process.py` | `src/ReadProcess.py` | Unitario + integración |
| `test_localization.py` | `src/localization.py` | Unitario |
| `test_pipeline.py` | `main.py` | Integración end-to-end |
| `test_utils.py` | `src/utils.py` | Unitario |
| `test_validate.py` | `src/validate.py` | Unitario |
| `conftest.py` | Fixtures y mocks | Infraestructura de tests |
 
### 18.2 Fixtures y mocks
 
`conftest.py` provee DataFrames mínimos, IPs de los rangos de documentación RFC 5737, directorios temporales (`tmp_path`) y un mock de `geoip2.database.Reader` con atributos primitivos explícitos (strings, floats) para evitar problemas de serialización con PyArrow.
 
### 18.3 Comandos
 
```bash
# Suite completa
pytest
 
# Solo unitarios
pytest -m unit
 
# Solo integración
pytest -m integration
 
# Con cobertura HTML
pytest --cov=src --cov-report=html
```
 
El reporte HTML se genera en `htmlcov/` y no forma parte de la salida del pipeline.
 
---
 
## 19. Notas de reconciliación (puntos a verificar)
 
Las dos documentaciones originales del proyecto presentaban diferencias en los siguientes puntos. Se listan aquí para que se confirmen contra el código fuente actual antes de tratar este documento como definitivo:
 
1. **Categorías de `dim_device`.** Una fuente indica 4 categorías (`mobile`, `desktop`, `web`, `other`) y la otra 5 (agregando `unknown`). Verificar el cuerpo de `classify_device()` en `src/ReadProcess.py`.
2. **Número de fases orquestadas por `main.py`.** Una fuente describe 3 etapas (ETL, geolocalización, reporte) y la otra 4 (agregando una pre-validación explícita de rutas como fase independiente y potencialmente fatal). Verificar la función principal de `main.py` y si el fallo de `src/utils.py` efectivamente detiene la ejecución.
3. **Condición de fallo de `extract_json_files()`.** Una fuente indica que falla si la ruta no existe *o* no contiene entradas válidas; la otra solo documenta la ausencia de archivos `.json`. Verificar si existe una validación adicional sobre el contenido de los JSON más allá de su presencia.
4. **Cifra de cobertura de tests ("superior al 80%").** Solo una de las fuentes originales incluía este dato. Se recomienda confirmarlo contra el reporte de cobertura (`htmlcov/`) más reciente antes de publicarlo como cifra fija, ya que puede desactualizarse con el tiempo.
---
 
## 20. Resumen técnico
 
El proyecto implementa un pipeline ETL completo que transforma el historial de reproducción de Spotify desde datos JSON semi-estructurados hacia un modelo dimensional orientado al análisis, combinando Python, pandas, Apache Parquet, un esquema en estrella, enriquecimiento geográfico con MaxMind GeoLite2 y una suite de tests con pytest.
 
El resultado es un conjunto de datasets estructurados y testeados, preparados para su consumo mediante herramientas de Business Intelligence como Power BI. La arquitectura modular (`config.py`, `main.py`, `src/ReadProcess.py`, `src/localization.py`, `src/validate.py`, `src/utils.py`, `src/logger.py`) permite incorporar nuevas dimensiones, fuentes de enriquecimiento o etapas de transformación sin modificar significativamente el núcleo existente.