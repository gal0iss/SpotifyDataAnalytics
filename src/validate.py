"""Validación de datos enriquecidos de geolocalización."""

import pandas as pd
from pathlib import Path
from .logger import logger
from config import DIM_LOCATION_ENRICHED_FILE

def validate_data() -> None:
    """Valida los datos enriquecidos de ubicación.
    
    Verifica:
    - Cantidad de registros
    - Cobertura de ciudades, regiones, ISPs
    - Top 10 ciudades y compañías
    - Muestra de registros
    """
    if not DIM_LOCATION_ENRICHED_FILE.exists():
        logger.warning(f"El archivo enriquecido no existe: {DIM_LOCATION_ENRICHED_FILE}")
        return

    # Leer el archivo
    try:
        df = pd.read_parquet(DIM_LOCATION_ENRICHED_FILE)
    except Exception as e:
        logger.error(f"Error leyendo archivo de validación: {e}", exc_info=True)
        return

    logger.info("\n---  RESUMEN DE ENRIQUECIMIENTO ---")
    logger.info(f"Total de registros: {len(df)}")
    
    # 1. Verificar nulos (deberían ser 'Unknown' si el script anterior funcionó bien)
    logger.info("\n Conteo de valores por columna:")
    cols = ['city', 'region', 'isp', 'latitude']
    for col in cols:
        if col in df.columns:
            try:
                if df[col].dtype == object:
                    count = (df[col] != "Unknown").sum()
                else:
                    count = (df[col] != 0).sum()
                logger.info(f"   {col.capitalize()}: {count} registros encontrados")
            except Exception as e:
                logger.warning(f"   Error contando {col}: {e}")
        else:
            logger.warning(f"   Columna '{col}' no encontrada")

    # 2. Ver el Top de ISPs (Compañías de internet)
    if 'isp' in df.columns:
        logger.info("\n TOP 10 COMPAÑÍAS DE INTERNET (ISP):")
        try:
            isp_top = df[df['isp'] != "Unknown"]['isp'].value_counts().head(10)
            for idx, (isp, count) in enumerate(isp_top.items(), 1):
                logger.info(f"   {idx}. {isp}: {count}")
        except Exception as e:
            logger.warning(f"   Error procesando ISPs: {e}")

    # 3. Ver el Top de Ciudades
    if 'city' in df.columns:
        logger.info("\n TOP 10 CIUDADES DETECTADAS:")
        try:
            cities_top = df[df['city'] != "Unknown"]['city'].value_counts().head(10)
            for idx, (city, count) in enumerate(cities_top.items(), 1):
                logger.info(f"   {idx}. {city}: {count}")
        except Exception as e:
            logger.warning(f"   Error procesando ciudades: {e}")

    # 4. Muestra de las primeras filas
    logger.info("\n MUESTRA DE DATOS:")
    try:
        cols_to_show = [col for col in ['ip_addr', 'city', 'region', 'isp'] if col in df.columns]
        sample = df[cols_to_show].head(5).to_string()
        logger.info(f"\n{sample}")
    except Exception as e:
        logger.warning(f"   Error mostrando muestra: {e}")

if __name__ == "__main__":
    try:
        validate_data()
    except Exception as e:
        logger.error(f"Error en validación: {e}", exc_info=True)
