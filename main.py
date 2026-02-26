"""
Orquestador principal del pipeline ETL de Spotify.

Ejecuta en orden:
1. Validación de entrada
2. Extract & Transform
3. Enriquecimiento geográfico (opcional)
4. Validación de calidad
"""

import sys
from pathlib import Path
from src.logger import logger
from src.utils import validate_input_directory, ensure_output_directory
from src.localization import enrich_location_dimension
from src.validate import validate_data    
from src.ReadProcess import main as etl_main
from config import RAW_DATA_DIR, PROCESSED_DATA_DIR, LOGS_DIR

# ============================================
# PRE-VALIDACIÓN: Verificar rutas y permisos
# ============================================
logger.info(" VALIDANDO CONFIGURACIÓN Y RUTAS")


try:
    if not validate_input_directory(RAW_DATA_DIR, "*.json"):
        logger.error(f"Directorio de entrada no válido: {RAW_DATA_DIR}")
        sys.exit(1)
    
    if not ensure_output_directory(PROCESSED_DATA_DIR):
        logger.error(f"No se pudo crear directorio de salida: {PROCESSED_DATA_DIR}")
        sys.exit(1)
    
    if not ensure_output_directory(LOGS_DIR):
        logger.error(f"No se pudo crear directorio de logs: {LOGS_DIR}")
        sys.exit(1)
    
    logger.info("✓ Rutas validadas correctamente")
except Exception as e:
    logger.error(f"Error en validación de configuración: {e}", exc_info=True)
    sys.exit(1)

# ============================================
# PASO 1: EXTRACT & TRANSFORM
# ============================================

logger.info(" PASO 1: EXTRACT & TRANSFORM")

try:
    etl_main()
    logger.info(" ETL completado exitosamente")
except Exception as e:
    logger.error(f" Error en ETL: {e}", exc_info=True)
    sys.exit(1)

# ============================================
# PASO 2: ENRIQUECIMIENTO CON GEOLOCALIZACIÓN
# ============================================
logger.info(" PASO 2: ENRIQUECIMIENTO GEOGRÁFICO")

try:
    enrich_location_dimension()
    logger.info("✓ Enriquecimiento de geolocalización completado")
except Exception as e:
    logger.warning(f" No se pudo enriquecer datos geográficos: {e}")
    logger.info("   Continuando sin enriquecimiento (requiere GeoLite2)...")

# ============================================
# PASO 3: VALIDACIÓN DE DATOS
# ============================================

logger.info(" PASO 3: VALIDACIÓN DE DATOS")


try:
    validate_data()
    logger.info("✓ Validación completada")
except Exception as e:
    logger.warning(f" Advertencia en validación: {e}")

# ============================================
# RESUMEN FINAL
# ============================================
logger.info(" PIPELINE COMPLETADO")
logger.info("\n Datos procesados guardados en:")
logger.info(f"   → {PROCESSED_DATA_DIR}")
logger.info("\n Archivos generados:")
logger.info("   - dim_date.parquet")
logger.info("   - dim_device.parquet")
logger.info("   - dim_track.parquet")
logger.info("   - dim_episode.parquet")
logger.info("   - dim_location.parquet")
logger.info("   - dim_location_enriched.parquet (si aplicó geolocalización)")
logger.info("   - fact_table.parquet")
logger.info("\n Log de ejecución guardado en:")
logger.info(f"   → {LOGS_DIR}")
logger.info("\n Listo para análisis")
