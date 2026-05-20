# Spotify Portfolio - Análisis de datos

Pipeline ETL que procesa historiales JSON de Spotify y genera dimensiones
(fecha, dispositivo, track, episodio, ubicación) y una tabla de hechos con
eventos de reproducción. Incluye enriquecimiento de IPs usando bases
GeoLite2. Pensado para hacer un análisis de datos mediante Power BI.

## Estructura principal

```
portafolio-spotify/
├── src/
│   ├── ReadProcess.py
│   ├── localization.py
│   └── validate.py
├── data/
│   ├── raw/
│   ├── processed/
│   └── Databases/
├── tests/
│   ├──init
│   ├── confest
│   ├── README.md
│   ├── test_localization
│   ├── test_logger
│   ├── test_main
│   ├── test_pipeline
│   ├── test_read_process
│   └── test_utils
├── main.py
├── requirements.txt
└── README.md
```

## Uso 

1. Crear y activar entorno virtual.
2. `pip install -r requirements.txt`.
3. Colocar los JSON de historial en `data/raw/`.
4. Opcional: añadir bases GeoLite2 en `data/Databases/`.
5. Ejecutar `python main.py`.

El pipeline realiza extracción, transformación, enriquecimiento y
validación. Los resultados se escriben en `data/processed/` como archivos
Parquet.

## Tests

Ver `tests/README.md` para instrucciones completas. Se incluyen pruebas
unitarias e integración; se pueden ejecutar con `pytest` o
`python run_tests.py`.

## Notas

- La carpeta `data/` no se versiona.
- El proceso es idempotente y tolerante a errores.
- Los mensajes se muestran en la consola.

---

Última actualización: Marzo 2026
