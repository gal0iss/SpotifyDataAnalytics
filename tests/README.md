# Tests del pipeline ETL

Repositorio de pruebas para el proyecto.

## Archivos

- `tests/conftest.py`: fixtures comunes
- `tests/test_read_process.py`: ETL
- `tests/test_localization.py`: geolocalización
- `tests/test_pipeline.py`: integración end-to-end
- `pytest.ini`: configuración de pytest

## Ejecución

```bash
pytest                # todos los tests
pytest -m unit        # solo unitarios
pytest -m integration # solo integración
pytest --cov=src --cov-report=html
```

El informe de cobertura queda en `htmlcov/index.html`.

## Qué se prueba

Unitarios cubren extracción, limpieza, dimensiones, hechos y
persistencia. Geoip enriquece IPs y maneja errores. Integración valida
flujo completo y referencias.

## Fixtures importantes

- `sample_streaming_data`
- `sample_streaming_data_with_podcasts`
- `temp_data_dir` (tmp_path)
- `mock_geoip2_reader`

## Convenciones

Tests siguen patrón Arrange/Act/Assert y usan docstrings
GIVEN/WHEN/THEN. Nombres descriptivos y fixtures reutilizables.

## Objetivo

Demostrar habilidades en pytest, mocks, validación de datos y pruebas
de pipelines de datos.

