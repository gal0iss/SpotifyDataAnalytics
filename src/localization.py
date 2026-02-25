"""Enriquecimiento de datos geográficos: Geolocalización de IPs con MaxMind."""

import pandas as pd
import geoip2.database
from pathlib import Path
from .logger import logger
from config import DIM_LOCATION_FILE, DIM_LOCATION_ENRICHED_FILE, DB_DIR
from config import CITY_DB, ASN_DB, COUNTRY_DB

def enrich_location_dimension() -> None:
    """Enriquece dimensión de ubicación con datos geográficos de MaxMind.
    
    Consulta bases GeoLite2 para agregar: ciudad, región, ISP, coordenadas
    a la dimensión de ubicación. Maneja gracefully IPs no encontradas.
    """
    # 1. Cargar la dimensión actual
    if not DIM_LOCATION_FILE.exists():
        logger.error(f"No se encontró: {DIM_LOCATION_FILE}")
        raise FileNotFoundError(f"Error: No se encontró {DIM_LOCATION_FILE}")

    dim_location = pd.read_parquet(DIM_LOCATION_FILE)
    logger.info(f"✓ Iniciando enriquecimiento para {len(dim_location)} registros")

    # 2. Obtener IPs únicas para optimizar (evitamos re-consultar la misma IP)
    # Filtramos la IP '0.0.0.0' o 'Unknown' si existen
    ips_unicas = dim_location[~dim_location['ip_addr'].isin(['0.0.0.0', 'unknown', 'Unknown'])]['ip_addr'].unique()
    
    geo_data = []

    # 3. Abrir los lectores de MaxMind
    try:
        with geoip2.database.Reader(str(CITY_DB)) as city_reader, \
            geoip2.database.Reader(str(ASN_DB)) as asn_reader, \
                geoip2.database.Reader(str(COUNTRY_DB)) as country_reader:
            for ip in ips_unicas:
                try:
                    # Consulta de Ciudad y Coordenadas
                    res_city = city_reader.city(ip)
                    # Consulta de ISP (Compañía)
                    res_asn = asn_reader.asn(ip)
                    # Consulta de País
                    res_country = country_reader.country(ip)
                    
                    geo_data.append({
                        "ip_addr": ip,
                        "city": res_city.city.name,
                        "country": res_country.country.name,
                        "region": res_city.subdivisions.most_specific.name,
                        "latitude": res_city.location.latitude,
                        "longitude": res_city.location.longitude,
                        "isp": res_asn.autonomous_system_organization,
                    })
                except Exception as e:
                    # Si la IP no está en la base de datos (IPs privadas o locales)
                    logger.debug(f"IP no encontrada en BD: {ip}")
                    continue

    except FileNotFoundError as e:
        logger.error(f"Bases GeoLite2 no encontradas en: {DB_DIR}")
        logger.error(f"Detalle: {e}")
        raise

    # 4. Crear DataFrame con info nueva y unir con la original
    df_geo = pd.DataFrame(geo_data)
    logger.info(f"✓ {len(df_geo)} IPs enriquecidas geográficamente")
    dim_enriched = pd.merge(dim_location, df_geo, on="ip_addr", how="left")

    # 5. Rellenar nulos para el Miembro Desconocido o fallos
    cols_to_fix = ["city", "region", "isp"]
    for col in cols_to_fix:
        dim_enriched[col] = dim_enriched[col].fillna("Unknown")
    
    dim_enriched["latitude"] = dim_enriched["latitude"].fillna(0)
    dim_enriched["longitude"] = dim_enriched["longitude"].fillna(0)

    # 6. Guardar la versión final
    try:
        dim_enriched.to_parquet(DIM_LOCATION_ENRICHED_FILE, index=False)
        logger.info(f"✓ Dimensión enriquecida guardada: {DIM_LOCATION_ENRICHED_FILE.name}")
        logger.info(f"   Campos: City, Region, Latitude, Longitude, ISP")
    except Exception as e:
        logger.error(f"Error guardando dim_location_enriched: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    try:
        enrich_location_dimension()
    except Exception as e:
        logger.error(f"Error en geolocalización: {e}", exc_info=True)