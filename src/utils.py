"""
Utilidades comunes reutilizables en todo el pipeline.
"""

from pathlib import Path
from .logger import logger


def validate_input_file(file_path: Path, file_type: str = "file") -> bool:
    """
    Valida la existencia y accesibilidad de un archivo.
    
    Args:
        file_path: Ruta del archivo
        file_type: Descripción del tipo de archivo
        
    Returns:
        True si el archivo es válido, False en caso contrario
    """
    if not file_path.exists():
        logger.error(f"{file_type} no existe: {file_path}")
        return False
    
    if not file_path.is_file():
        logger.error(f"{file_type} no es un archivo válido: {file_path}")
        return False
    
    logger.debug(f"✓ {file_type} validado: {file_path}")
    return True


def validate_input_directory(dir_path: Path, must_contain: str = None) -> bool:
    """
    Valida la existencia y contenido de un directorio.
    
    Args:
        dir_path: Ruta del directorio
        must_contain: Patrón de archivos que debe contener (ej: "*.json")
        
    Returns:
        True si el directorio es válido, False en caso contrario
    """
    if not dir_path.exists():
        logger.error(f"Directorio no existe: {dir_path}")
        return False
    
    if not dir_path.is_dir():
        logger.error(f"No es un directorio válido: {dir_path}")
        return False
    
    if must_contain:
        files = list(dir_path.glob(must_contain))
        if not files:
            logger.error(f"Directorio vacío o sin archivos '{must_contain}': {dir_path}")
            return False
        logger.debug(f"✓ Directorio contiene {len(files)} archivos: {must_contain}")
    
    logger.debug(f"✓ Directorio validado: {dir_path}")
    return True


def ensure_output_directory(output_dir: Path) -> bool:
    """
    Se asegura de que el directorio de salida existe.
    
    Args:
        output_dir: Ruta del directorio de salida
        
    Returns:
        True si el directorio está listo, False en caso de error
    """
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        logger.debug(f" Directorio de salida listo: {output_dir}")
        return True
    except Exception as e:
        logger.error(f"No se pudo crear directorio: {output_dir}. Error: {e}")
        return False
