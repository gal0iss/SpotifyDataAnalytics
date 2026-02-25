"""Procesamiento ETL: Extract, Transform, Load datos de Spotify."""

from pathlib import Path
import pandas as pd
import numpy as np
from .logger import logger
from config import RAW_DATA_DIR, PROCESSED_DATA_DIR, AUDIOBOOK_COLUMNS_PATTERN
from config import DIM_DATE_FILE, DIM_DEVICE_FILE, DIM_TRACK_FILE, DIM_EPISODE_FILE
from config import DIM_LOCATION_FILE, FACT_TABLE_FILE, UNKNOWN_VALUE, UNKNOWN_ID

# ==============================
# EXTRACT
# ==============================

def extract_json_files(path: Path) -> pd.DataFrame:
    """Extrae y concatena todos los JSONs de Spotify desde un directorio.
    
    Args:
        path: Ruta del directorio contiendo archivos JSON
        
    Returns:
        DataFrame con todos los registros concatenados
        
    Raises:
        FileNotFoundError: Si la ruta no existe o está vacía
    """
    if not path.exists():
        logger.error(f"Ruta no existe: {path.resolve()}")
        raise FileNotFoundError(f"No existe la ruta: {path.resolve()}")
    files = list(path.glob("*.json"))

    if not files:
        logger.error(f"No se encontraron JSONs en: {path.resolve()}")
        raise FileNotFoundError(f"No se encontraron JSON en: {path.resolve()}")
    logger.info(f"✓ Se encontraron {len(files)} archivos JSON")
    try:
        df = pd.concat(
            (pd.read_json(f, convert_dates=["ts"]) for f in files),
            ignore_index=True
        )
        logger.info(f"✓ Extract completado: {df.shape[0]} registros, {df.shape[1]} columnas")
        return df
    except Exception as e:
        logger.error(f"Error al leer JSONs: {e}", exc_info=True)
        raise

def save_to_parquet(df: pd.DataFrame, output_file: Path) -> None:
    """Guarda un DataFrame en formato Parquet.
    
    Args:
        df: DataFrame a guardar
        output_file: Ruta de salida del archivo
        
    Raises:
        Exception: Si hay error en la escritura
    """
    try:
        df.to_parquet(output_file, index=False)
        logger.info(f"✓ Tabla [{output_file.stem}] guardada: {output_file.name} ({len(df)} filas)")
    except Exception as e:
        logger.error(f"Error guardando {output_file.stem}: {e}", exc_info=True)
        raise

# ==============================
# CLEAN
# ==============================

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia datos: elimina columnas vacías y agrega event_id.
    
    Args:
        df: DataFrame a limpiar
        
    Returns:
        DataFrame limpio con column event_id
    """
    df = df.copy()
    audiobook_cols = [col for col in df.columns if AUDIOBOOK_COLUMNS_PATTERN in col]
    if audiobook_cols and df[audiobook_cols].isnull().all().all():
        df = df.drop(columns=audiobook_cols)
        logger.info(f"✓ Eliminadas {len(audiobook_cols)} columnas de audiobooks (vacías)")
    df = df.reset_index(drop=True)
    df["event_id"] = df.index
    logger.info(f"✓ Clean completado: {df.shape}")
    return df

# ==============================
# DIMENSIONS
# ==============================

def create_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    """Crea dimensión temporal con granularidad horaria.
    
    Args:
        df: DataFrame con columna 'ts' en formato datetime
        
    Returns:
        DataFrame dim_date con PK=date_id (YYYYMMDDHH)
    """
    temp = df.copy()
    temp["date_id"] = temp["ts"].dt.strftime("%Y%m%d%H")
    temp["year"] = temp["ts"].dt.year
    temp["month"] = temp["ts"].dt.month
    temp["day"] = temp["ts"].dt.day
    temp["weekday"] = temp["ts"].dt.day_name()
    temp["hour"] = temp["ts"].dt.hour

    # Forzamos unicidad por la clave natural date_id
    dim_date = temp[[
        "date_id", "year", "month", "day", "weekday", "hour"
    ]].drop_duplicates(subset=["date_id"]).reset_index(drop=True)
    
    logger.debug(f"✓ dim_date creada: {dim_date.shape}")
    return dim_date

def classify_device(platform: str | None) -> str:
    """Clasifica dispositivo según plataforma.
    
    Args:
        platform: Nombre de la plataforma
        
    Returns:
        Tipo de dispositivo: 'mobile', 'desktop', 'web', otros o 'unknown'
    """
    if pd.isna(platform): return "unknown"
    platform = str(platform).lower()
    if "android" in platform or "ios" in platform: return "mobile"
    elif "windows" in platform or "mac" in platform: return "desktop"
    elif "web" in platform: return "web"
    else: return "other"

def create_dim_device(df: pd.DataFrame) -> pd.DataFrame:
    """Crea dimensión de dispositivos con clasificación tipo.
    
    Args:
        df: DataFrame con columna 'platform'
        
    Returns:
        DataFrame dim_device con PK=device_id
    """
    temp = df.copy()
    temp["device_type"] = temp["platform"].apply(classify_device)
    
    # Unicidad por platform (Natural Key)
    dim_device = temp[["platform", "device_type"]].drop_duplicates(subset=["platform"]).reset_index(drop=True)
    dim_device["device_id"] = dim_device.index
    
    unknown = pd.DataFrame([{"platform": UNKNOWN_VALUE, "device_type": "unknown", "device_id": UNKNOWN_ID}])
    dim_device = pd.concat([unknown, dim_device], ignore_index=True)
    
    logger.debug(f"✓ dim_device creada: {dim_device.shape}")
    return dim_device

def create_dim_track(df: pd.DataFrame) -> pd.DataFrame:
    """Crea dimensión de tracks (canciones).
    
    Args:
        df: DataFrame con columnas de track Spotify
        
    Returns:
        DataFrame dim_track con PK=track_id
    """
    music_df = df[df["spotify_track_uri"].notna()].copy()
    
    # Unicidad por spotify_track_uri
    dim_track = music_df[[
        "spotify_track_uri",
        "master_metadata_track_name",
        "master_metadata_album_artist_name",
        "master_metadata_album_album_name"
    ]].drop_duplicates(subset=["spotify_track_uri"]).reset_index(drop=True)

    dim_track = dim_track.rename(columns={
        "master_metadata_track_name": "track_name",
        "master_metadata_album_artist_name": "artist_name",
        "master_metadata_album_album_name": "album_name"
    })
    dim_track["track_id"] = dim_track.index
    
    unknown = pd.DataFrame([{
        "spotify_track_uri": UNKNOWN_VALUE, "track_name": "Non-Music Content", 
        "artist_name": "N/A", "album_name": "N/A", "track_id": UNKNOWN_ID
    }])
    dim_track = pd.concat([unknown, dim_track], ignore_index=True)
    
    logger.debug(f"✓ dim_track creada: {dim_track.shape}")
    return dim_track

def create_dim_episode(df: pd.DataFrame) -> pd.DataFrame:
    """Crea dimensión de episodios (podcasts).
    
    Args:
        df: DataFrame con columnas de episode Spotify
        
    Returns:
        DataFrame dim_episode con PK=episode_id
    """
    podcast_df = df[df["spotify_episode_uri"].notna()].copy()
    
    # Unicidad por spotify_episode_uri
    dim_episode = podcast_df[[
        "spotify_episode_uri",
        "episode_name",
        "episode_show_name"
    ]].drop_duplicates(subset=["spotify_episode_uri"]).reset_index(drop=True)

    dim_episode = dim_episode.rename(columns={"episode_show_name": "show_name"})
    dim_episode["episode_id"] = dim_episode.index
    
    unknown = pd.DataFrame([{
        "spotify_episode_uri": UNKNOWN_VALUE, 
        "episode_name": "Non-Podcast Content", 
        "show_name": "N/A", 
        "episode_id": UNKNOWN_ID
    }])
    
    dim_episode = pd.concat([unknown, dim_episode], ignore_index=True)
    logger.debug(f"✓ dim_episode creada: {dim_episode.shape}")
    return dim_episode

def create_dim_location(df: pd.DataFrame) -> pd.DataFrame:
    """Crea dimensión de ubicación (IPs y países).
    
    Args:
        df: DataFrame con columnas 'ip_addr' y 'conn_country'
        
    Returns:
        DataFrame dim_location con PK=location_id
    """
    # Unicidad por ip_addr (Natural Key)
    dim_location = df[["ip_addr", "conn_country"]].drop_duplicates(subset=["ip_addr"]).reset_index(drop=True)
    dim_location["location_id"] = dim_location.index
    
    unknown = pd.DataFrame([{"ip_addr": "0.0.0.0", "conn_country": "XX", "location_id": UNKNOWN_ID}])
    dim_location = pd.concat([unknown, dim_location], ignore_index=True)
    
    logger.debug(f"✓ dim_location creada: {dim_location.shape}")
    return dim_location

# ==============================
# FACT TABLE
# ==============================

def create_fact_table(df: pd.DataFrame) -> pd.DataFrame:
    """Crea tabla de hechos con eventos de reproducción (Star Schema).
    
    Args:
        df: DataFrame inicial con todas las columnas y FKs mergeadas
        
    Returns:
        DataFrame fact_table con eventos de reproducción
    """
    temp = df.copy()
    temp["date_id"] = temp["ts"].dt.strftime("%Y%m%d%H")

    fact = temp[[
        "event_id", "date_id", "track_id", "episode_id", 
        "device_id", "location_id", "ms_played", 
        "skipped", "shuffle", "offline", "incognito_mode"
    ]].copy()

    # Rellenar nulos en FKs con UNKNOWN_ID
    fact["track_id"] = fact["track_id"].fillna(UNKNOWN_ID).astype(int)
    fact["episode_id"] = fact["episode_id"].fillna(UNKNOWN_ID).astype(int)
    fact["device_id"] = fact["device_id"].fillna(UNKNOWN_ID).astype(int)
    fact["location_id"] = fact["location_id"].fillna(UNKNOWN_ID).astype(int)
    
    # Asegurar tipos booleanos para columnas binarias antes de escribir a parquet
    bool_cols = ["skipped", "shuffle", "offline", "incognito_mode"]
    for col in bool_cols:
        if col in fact.columns:
            fact[col] = pd.to_numeric(fact[col], errors="coerce").fillna(0).astype(bool)

    logger.debug(f"✓ fact_table creada: {fact.shape}")
    return fact

# ==============================
# MAIN
# ==============================

def main() -> None:
    """Orquesta el pipeline completo ETL: Extract, Transform, Load."""
    # 1. EXTRACT & CLEAN
    df = extract_json_files(RAW_DATA_DIR)
    df = clean_data(df)

    # 2. CREATE DIMENSIONS
    dim_date = create_dim_date(df)
    dim_device = create_dim_device(df)
    dim_track = create_dim_track(df)
    dim_episode = create_dim_episode(df)
    dim_location = create_dim_location(df)

    # 3. MERGES (Solo contra las Natural Keys únicas para evitar duplicar filas)
    # Importante: No incluimos columnas de atributos en el merge, solo la natural key y el id.
    
    df = df.merge(dim_device[["platform", "device_id"]], on="platform", how="left")
    df = df.merge(dim_track[["spotify_track_uri", "track_id"]], on="spotify_track_uri", how="left")
    df = df.merge(dim_episode[["spotify_episode_uri", "episode_id"]], on="spotify_episode_uri", how="left")
    df = df.merge(dim_location[["ip_addr", "location_id"]], on="ip_addr", how="left")

    # 4. FACT TABLE
    fact_table = create_fact_table(df)

    # 5. FINAL VALIDATION
    logger.info("--- VALIDACIÓN FINAL ---")
    logger.info(f"Filas originales: {len(df)}")
    logger.info(f"Filas Fact Table: {len(fact_table)}")
    
    keys_to_check = ["track_id", "episode_id", "device_id", "location_id"]
    nulls = fact_table[keys_to_check].isnull().sum()
    if nulls.sum() > 0:
        logger.warning(f"Valores nulos encontrados en Fact Table: {nulls.to_dict()}")
    else:
        logger.info("✓ Sin valores nulos en claves foráneas")

    # LOAD: Save all to Parquet
    save_to_parquet(dim_date, DIM_DATE_FILE)
    save_to_parquet(dim_device, DIM_DEVICE_FILE)
    save_to_parquet(dim_track, DIM_TRACK_FILE)
    save_to_parquet(dim_episode, DIM_EPISODE_FILE)
    save_to_parquet(dim_location, DIM_LOCATION_FILE)
    save_to_parquet(fact_table, FACT_TABLE_FILE)
    


if __name__ == "__main__":
    main()