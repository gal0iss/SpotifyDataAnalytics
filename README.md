# 🎵 Spotify Portfolio - Análisis e Ingeniería de Datos

Proyecto de análisis de histórico de reproducción de Spotify usando arquitectura simple de datos (ETL).

##  Descripción

Pipeline de datos que procesa archivos JSON del historial de Spotify y genera:
- **Dimensiones**: fecha, dispositivo, track, episodio, ubicación
- **Tabla de Hechos**: eventos de reproducción
- **Enriquecimiento**: geolocalización de IPs (ciudad, ISP, coordenadas)

##  Estructura del Proyecto

```
portafolio-spotify/
├── main.py                    #  Orquestador principal (START HERE)
├── README.md                  # Este archivo
├── requirements.txt           # Dependencias Python
├── data/
│   ├── raw/                   #  JSONs originales del exporto de Spotify
│   ├── processed/             #  Archivos Parquet procesados
│   └── Databases/             #  Bases geográficas MaxMind (GeoLite2)
└── src/
    ├── ReadProcess.py         # Extract + Transform (dimensiones, hechos)
    ├── localization.py        # Enriquecimiento: geolocalización de IPs
    └── validate.py            # Validación: calidad de datos
```

##  Cómo usar

### 1. Preparar ambiente

```bash
# Crear entorno virtual (si no existe)
python -m venv .venv

# Activar
.venv\Scripts\Activate.ps1     # Windows PowerShell
source .venv/bin/activate      # Linux/Mac

# Instalar dependencias
pip install -r requirements.txt
```

### 2. Ubicar datos

Coloca tus JSONs del historial de Spotify en `data/raw/`:
- `Streaming_History_Audio_YYYY-YYYY_X.json`

> Si trabajas con varias cuentas o quieres mantener directorios separados,
> define la variable de entorno `SPOTIFY_USER_SUFFIX` (ej. `Pedro`) antes de
> ejecutar el pipeline; el script ajustará automáticamente `data/raw{suffix}`
> y `data/processed{suffix}`.

Nota: Requiere bases de datos GeoLite2 en `data/Databases/` (descargar de MaxMind)

### 3. Ejecutar pipeline

```bash
python main.py
```

El script ejecutará en orden:
1. **Extract & Transform** - Procesa JSONs y crea dimensiones
2. **Enrich** - Geolocaliza IPs (opcional - requiere GeoLite2)
3. **Validate** - Verifica calidad de datos

## Salidas

Archivos Parquet en `data/processed/`:

| Archivo | Descripción |
|---------|-------------|
| `dim_date.parquet` | Dimensión temporal (año, mes, día, hora, día semana) |
| `dim_device.parquet` | Dispositivos (móvil, desktop, web) |
| `dim_track.parquet` | Tracks (URI, nombre, artista, álbum) |
| `dim_episode.parquet` | Episodios de podcasts |
| `dim_location.parquet` | Ubicación (IP, país) |
| `dim_location_enriched.parquet` | Ubicación enriquecida (ciudad, ISP, lat/lon) |
| `fact_table.parquet` | Tabla de hechos (reproduciones) |

## � Archivos de registro

Los mensajes del pipeline se envían tanto a la consola como a un fichero de
texto dentro de la carpeta `logs/` (se crea automáticamente). Revisa los logs
para diagnosticar errores o verificar pasos intermedios.

## Métrica Principal

Tabla de hechos estructura tipo **Star Schema**:

```
fact_table
├── event_id (PK)
├── date_id (FK → dim_date)
├── track_id (FK → dim_track)
├── episode_id (FK → dim_episode)
├── device_id (FK → dim_device)
├── location_id (FK → dim_location)
├── ms_played
├── skipped
├── shuffle
├── offline
└── incognito_mode
```

##  Dependencias

- `pandas` - procesamiento de datos
- `numpy` - cálculos numéricos
- `geoip2` - geolocalización de IPs
- `pyarrow` - soporte Parquet

Ver `requirements.txt` para versiones exactas.

##  Notas

- **Datos locales**: No se versiona `data/` (configurado en `.gitignore`)
- **Debugging**: Ver logs en consola de cada paso del pipeline
- **Manejo de errores**: El script continúa si falla la geolocalización
- **Idempotencia**: Puedes ejecutar `main.py` múltiples veces sin problemas

## 🎓 Propósito

Portfolio técnico demostrando:
- ✅ Ingeniería de datos (ETL)
- ✅ Modelado dimensional (Star Schema)
- ✅ Limpieza y validación de datos
- ✅ GIS/Geolocalización
- ✅ Pandas y procesamiento de datos
- ✅ Estructuración de proyectos Python

---

**Última actualización**: Feb 2026
