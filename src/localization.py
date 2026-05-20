"""Enriquecimiento de datos geográficos: Geolocalización de IPs con MaxMind."""

import ipaddress
import pandas as pd
import geoip2.database
from .logger import logger
from config import DIM_LOCATION_FILE, DIM_LOCATION_ENRICHED_FILE, DB_DIR
from config import CITY_DB, ASN_DB

_GEO_COLUMNS = ("ip_addr", "city", "country", "region", "latitude", "longitude", "isp")

_RFC1918_IPV4 = (
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
)


def _is_rfc1918_ipv4(addr: ipaddress.IPv4Address) -> bool:
    return any(addr in net for net in _RFC1918_IPV4)


def _skip_geolocation_ip(ip: str) -> bool:
    """True si no debemos consultar MaxMind (placeholder, RFC 1918 IPv4, loopback, etc.)."""
    if ip in ("0.0.0.0", "unknown", "Unknown"):
        return True
    try:
        addr = ipaddress.ip_address(str(ip).strip())
    except ValueError:
        return True
    if isinstance(addr, ipaddress.IPv4Address):
        return bool(
            _is_rfc1918_ipv4(addr)
            or addr.is_loopback
            or addr.is_link_local
            or addr.is_unspecified
            or addr.is_multicast
        )
    # IPv6: is_private incluye ULA; rangos de documentación no deben bloquear tests IPv4 RFC 5737
    return bool(
        addr.is_private
        or addr.is_loopback
        or addr.is_link_local
        or addr.is_unspecified
        or addr.is_multicast
    )


def enrich_location_dimension() -> None:
    """Enriquece dimensión de ubicación con datos geográficos de MaxMind.
    
    Consulta bases GeoLite2-City y GeoLite2-ASN para agregar: ciudad, región, país, 
    ISP y coordenadas a la dimensión de ubicación. Maneja gracefully IPs no encontradas.
    
    IMPORTANTE - Columnas de País:
    - conn_country: Código ISO 2 letras reportado por Spotify (ej: "US", "ES")
    - country: Nombre completo del país obtenido de MaxMind (ej: "United States", "Spain")
    
    Ambas columnas coexisten en la tabla enriquecida para diferentes propósitos analíticos.
    En Power BI, conn_country es útil para join con tablas de Spotify, mientras que 
    country proporciona nombres legibles en reportes.
    """
    # 1. Cargar la dimensión actual
    # Esta tabla ya contiene ip_addr (clave) y conn_country (código ISO de Spotify)
    if not DIM_LOCATION_FILE.exists():
        logger.error(f"No se encontró: {DIM_LOCATION_FILE}")
        raise FileNotFoundError(f"Error: No se encontró {DIM_LOCATION_FILE}")

    dim_location = pd.read_parquet(DIM_LOCATION_FILE)
    logger.info(f"✓ Iniciando enriquecimiento para {len(dim_location)} registros")

    # 2. Obtener IPs únicas para optimizar (evitamos re-consultar la misma IP)
    # Sin consultar MaxMind: placeholders, RFC 1918 (solo IPv4 explícito), loopback, etc.
    ips_unicas = [
        ip for ip in dim_location["ip_addr"].unique() if not _skip_geolocation_ip(ip)
    ]
    
    geo_data = []

    # 3. Abrir los lectores de MaxMind
    try:
        with geoip2.database.Reader(str(CITY_DB)) as city_reader, \
            geoip2.database.Reader(str(ASN_DB)) as asn_reader:
            for ip in ips_unicas:
                try:
                    # Consulta de Ciudad, País y Coordenadas (todo de CITY_DB)
                    res_city = city_reader.city(ip)
                    # Consulta de ISP (Compañía) - solo de ASN_DB
                    res_asn = asn_reader.asn(ip)
                    
                    geo_data.append({
                        "ip_addr": ip,
                        "city": res_city.city.name,
                        "country": res_city.country.name,
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

    # Se agregan: city, country (nombre completo), region, latitude, longitude, isp
    df_geo = pd.DataFrame(geo_data)
    if df_geo.empty:
        df_geo = pd.DataFrame(columns=list(_GEO_COLUMNS))
    logger.info(f"✓ {len(df_geo)} IPs enriquecidas geográficamente")
    # NOTA: Mantiene conn_country original (Spotify) + agrega country (MaxMind)
    dim_enriched = pd.merge(dim_location, df_geo, on="ip_addr", how="left")

    # 5. Rellenar nulos para el Miembro Desconocido o fallos
    cols_to_fix = ["city", "region", "isp"]
    for col in cols_to_fix:
        dim_enriched[col] = dim_enriched[col].fillna("Unknown")
    
    dim_enriched["latitude"] = dim_enriched["latitude"].fillna(0)
    dim_enriched["longitude"] = dim_enriched["longitude"].fillna(0)

    # 6. Guardar la versión final
    try:
        dim_enriched.to_parquet(DIM_LOCATION_ENRICHED_FILE)
        logger.info(f"✓ Dimensión enriquecida guardada: {DIM_LOCATION_ENRICHED_FILE.name}")
        logger.info(f"   Campos: City, Region, Latitude, Longitude, ISP")
    except Exception as e:
        logger.error(f"Error guardando dim_location_enriched: {e}", exc_info=True)
        raise

def _main_cli() -> None:
    try:
        enrich_location_dimension()
    except Exception as e:
        logger.error(f"Error en geolocalización: {e}", exc_info=True)


if __name__ == "__main__":
    _main_cli()