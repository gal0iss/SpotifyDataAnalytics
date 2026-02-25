"""
Logger profesional y centralizado para el pipeline de Spotify.
Compatible con logging estándar de Python.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime

# ============================================
# CONFIGURACIÓN DE LOGGER
# ============================================

def setup_logger(name: str = "spotify_pipeline") -> logging.Logger:
    """
    Configura un logger profesional con formato estándar.
    
    Args:
        name: Nombre del logger
        
    Returns:
        Logger configurado
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # Evitar duplicados si ya existe
    if logger.handlers:
        return logger
    
    # Formato profesional: timestamp, nivel, módulo, mensaje
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Handler: Consola (INFO y superior)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Handler: Archivo de log (DEBUG y superior)
    try:
        log_dir = Path(__file__).parent.parent / "logs"
        log_dir.mkdir(exist_ok=True)
        log_file = log_dir / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.debug(f"Log file: {log_file}")
    except Exception as e:
        logger.warning(f"No se pudo crear log file: {e}")
    
    return logger


# Logger global para el proyecto
logger = setup_logger("spotify_pipeline")
