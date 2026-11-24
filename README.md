# Listening Data · Portafolio Spotify

Convierte un historial de Spotify en un modelo analítico y permite explorar hábitos de escucha en el navegador. El flujo es **Spotify JSON → ETL Python → Parquet → SQL en DuckDB WASM → React + ECharts**. No requiere un servidor de consultas.


## Arrancar la demo

La demo usa los siete Parquet incluidos en [frontend/public/parquet](frontend/public/parquet/). No necesitas Python, JSON personales ni GeoLite2 para verla.

Con Node.js y npm instalados, desde la raíz, en PowerShell:

```powershell
cd frontend
npm.cmd ci
npm.cmd run dev
```

Abre la dirección que imprime Vite, con la ruta `/SpotifyDataAnalytics/`. Para generar y revisar el sitio compilado:

```powershell
npm.cmd run build
npm.cmd run preview
```

Instalación y build verificados con Node 26.2.0 y npm 11.13.0. En otras terminales puedes usar `npm` en lugar de `npm.cmd`. La ruta base está en [vite.config.ts](frontend/vite.config.ts). El [workflow de despliegue](.github/workflows/deploy.yml) compila con Node 20 y publica en GitHub Pages al recibir cambios en `main`; no ejecuta las pruebas.

## Explorar la escucha

**Resumen** muestra eventos, horas, tracks y artistas distintos y años observados. Un evento es una fila del historial: puede durar cero milisegundos y no implica que una canción se haya escuchado completa. Las horas son `SUM(ms_played) / 3600000`.

En **Hábitos de escucha**, elige un **año** y un **tipo de dispositivo** para comparar segmentos. No hay filtro de intervalo de fechas. Los gráficos ofrecen información al pasar el cursor; los botones **Ver SQL** permiten inspeccionar la consulta correspondiente.

| Vista o gráfico | Pregunta que responde y cómo leerlo |
| --- | --- |
| Indicadores de hábitos | ¿Cuándo y cómo escuchas? Hora y día con más eventos, minutos medios por evento, porcentaje de saltos y de uso de aleatorio. |
| Descubrimiento musical | ¿Qué domina el segmento? Canción y álbum más reproducidos, nombres de álbum distintos y proporción de artistas distintos por evento. Las canciones se identifican por `track_id`; los álbumes siguen contándose por nombre. |
| Artistas / tracks / álbumes nuevos | ¿Cómo crece el catálogo observado? Compara sus primeros años de aparición **dentro del segmento filtrado**; no son fechas de lanzamiento. |
| Constancia | ¿Qué tan regular es la escucha? Día y mes con más horas, días activos, días sin eventos entre la primera y última fecha observadas y racha máxima de días consecutivos. |
| Tiempo por año | ¿En qué años escuchaste más? Horas totales anuales. |
| Artistas por eventos y por tiempo | ¿Coinciden tus artistas más frecuentes y los que acumulan más horas? Compara ambos top 10. |
| Ritmo de escucha | ¿Qué horarios se repiten? El mapa de calor cruza día de la semana y hora y representa número de eventos. |
| Tracks principales | ¿Qué canciones repites más? Top 8 por eventos; títulos iguales de canciones distintas permanecen separados. |
| Comportamiento por dispositivo | ¿Qué cambia entre equipos? Compara eventos, horas y duración media. Respeta el año, pero mantiene todos los dispositivos visibles aunque selecciones uno. |
| Huella geográfica y ciudades | ¿Dónde hay escucha con ubicación conocida? Rankings de eventos por país y ciudad enriquecidos. Son barras, no un mapa geográfico; dependen de la cobertura de GeoLite2. |
| Calidad de datos | ¿Cuánta información queda sin identificar? Recuentos y porcentajes de claves desconocidas, cobertura de tracks y país enriquecido. Un track desconocido también puede corresponder a contenido no musical. |

### Detalle de artista

En **Explorar todos los artistas**, busca por nombre, abre la tabla completa si lo necesitas y selecciona una fila. El detalle conserva los filtros de año y dispositivo; **Cerrar detalle** vuelve a la exploración.

- **Resumen y comparación:** eventos, horas, tracks, nombres de álbum, primera y última escucha; participación en las horas del segmento, duración media y saltos comparados con la escucha global. También muestra uso de aleatorio y offline.
- **Evolución mensual:** horas y eventos por mes para localizar etapas de mayor actividad. El mes menos activo se elige entre los meses presentes en el segmento, no entre todos los meses del calendario.
- **Catálogo escuchado:** top 10 de canciones y álbumes. Ordena las filas visibles por nombre, eventos u horas desde las cabeceras. El porcentaje de una canción usa como denominador **todos** los eventos del artista; el top 10 no tiene por qué sumar 100 %.
- **Patrones:** compara distribuciones de eventos por día y hora, y de horas por dispositivo. Cada serie se normaliza por separado: artista frente a todo el segmento, que puede incluir podcasts. Con cero horas se muestra un estado sin datos para la distribución temporal, manteniendo los recuentos de eventos.
- **Países:** top 10 de países conocidos para ese artista, cuando hay cobertura geográfica.

**Modelo de datos** muestra tablas, campos y relaciones; pasar el cursor destaca conexiones. **Pipeline** explica las etapas de transformación. Los controles de idioma y tema permiten alternar español/inglés y claro/oscuro. Las consultas están en [queries.ts](frontend/src/data/queries.ts).

## Procesar un historial propio

Ejecuta el ETL solo cuando quieras generar o renovar tablas locales. La demo y el ETL son independientes: el pipeline no copia sus resultados al directorio público.

El entorno Python verificado es 3.14.3. Desde la raíz, instala las dependencias de [requirements.txt](requirements.txt):

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Coloca el historial extendido de Spotify en `data/raw/` como archivos JSON. El código espera los campos de eventos, plataformas, IP, metadatos de canciones y episodios del export extendido; [las fixtures de pruebas](tests/conftest.py) muestran una entrada sintética.

```powershell
.\.venv\Scripts\python.exe main.py
```

[main.py](main.py) valida rutas, ejecuta [el ETL](src/ReadProcess.py), intenta [el enriquecimiento](src/localization.py) y produce [un informe de validación](src/validate.py). El enriquecimiento requiere `GeoLite2-City.mmdb` y `GeoLite2-ASN.mmdb` en `data/Databases/`; si falla, el orquestador continúa con las tablas básicas. Revisa los avisos en `logs/`: terminar el proceso no garantiza que exista una dimensión enriquecida nueva.

Las rutas de [config.py](config.py) admiten conjuntos separados mediante un sufijo literal:

```powershell
$env:SPOTIFY_USER_SUFFIX = "Pedro"
.\.venv\Scripts\python.exe main.py
$env:SPOTIFY_USER_SUFFIX = ""
```

Esto lee `data/rawPedro/` y escribe `data/processedPedro/`; sin sufijo usa `data/raw/` y `data/processed/`. Las bases GeoLite2 son compartidas. Para ejecutar solo una etapa, usa el intérprete del entorno:

```powershell
.\.venv\Scripts\python.exe -m src.ReadProcess
.\.venv\Scripts\python.exe -m src.localization
.\.venv\Scripts\python.exe -m src.validate
```

## Modelo y salidas

La tabla **fact_table** conserva una fila por evento de entrada, con `event_id`, cinco claves de dimensión, `ms_played` y las banderas `skipped`, `shuffle`, `offline` e `incognito_mode`. `event_id` se asigna por posición durante cada ejecución; no es un identificador estable entre cargas.

| Archivo de salida | Grano / clave |
| --- | --- |
| `fact_table.parquet` | Un evento / `event_id`. |
| `dim_date.parquet` | Una hora observada / `date_id`, cadena `YYYYMMDDHH`. |
| `dim_device.parquet` | Una plataforma / `device_id`, con tipo de dispositivo. |
| `dim_track.parquet` | Una URI de canción / `track_id`. |
| `dim_episode.parquet` | Una URI de episodio / `episode_id`. |
| `dim_location.parquet` | Una IP / `location_id`, conservando `conn_country` de Spotify. |
| `dim_location_enriched.parquet` | La misma ubicación con país, ciudad, región, coordenadas e ISP; se genera si se completa GeoLite2. |

Las dimensiones de contenido, dispositivo y ubicación incluyen un miembro desconocido `-1`. Las uniones del ETL validan cardinalidad muchos-a-uno y el proceso comprueba la conservación de eventos antes de escribir. Los títulos de canciones no son claves; los nombres de álbum se usan como la métrica disponible, sin una dimensión propia de álbum.

Los datos locales, dependencias, logs y cobertura se excluyen en [.gitignore](.gitignore). Los Parquet públicos sí están versionados y el navegador puede descargarlos: que la interfaz presente agregados no elimina los campos de los archivos. Publicar un historial nuevo requiere revisar esos archivos; esta ronda conserva la demo existente.

## Pruebas

Desde la raíz:

```powershell
.\.venv\Scripts\python.exe -m pytest
cd frontend
npm.cmd test
npm.cmd run build
```

[pytest.ini](pytest.ini) activa cobertura de `src` y genera `htmlcov/`. La suite Python usa fixtures y rutas temporales, con GeoLite2 simulado. Las [regresiones del frontend](frontend/tests/analytics.test.cjs) ejecutan el SQL con DuckDB WASM sobre tablas sintéticas en memoria y renderizan el detalle de artista con React; no leen los Parquet públicos. Cubren títulos homónimos, identidad del catálogo y distribuciones con tiempo cero.
