"""
Tests para el módulo utils.

Verifica:
- Validación de archivos de entrada
- Validación de directorios de entrada
- Creación de directorios de salida
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch

from src.utils import (
    validate_input_file,
    validate_input_directory,
    ensure_output_directory,
)


class TestValidateInputFile:
    """Tests para validate_input_file."""

    def test_validate_input_file_success(self, tmp_path):
        """
        GIVEN: un archivo que existe
        WHEN: se ejecuta validate_input_file
        THEN: retorna True
        """
        # Arrange
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")
        
        # Act
        result = validate_input_file(test_file)
        
        # Assert
        assert result is True

    def test_validate_input_file_not_exists(self, tmp_path, mocker):
        """
        GIVEN: un archivo que no existe
        WHEN: se ejecuta validate_input_file
        THEN: retorna False
        """
        # Arrange
        nonexistent_file = tmp_path / "nonexistent.txt"
        mocker.patch("src.utils.logger")
        
        # Act
        result = validate_input_file(nonexistent_file)
        
        # Assert
        assert result is False

    def test_validate_input_file_is_directory(self, tmp_path, mocker):
        """
        GIVEN: una ruta que es un directorio, no un archivo
        WHEN: se ejecuta validate_input_file
        THEN: retorna False
        """
        # Arrange
        test_dir = tmp_path / "subdir"
        test_dir.mkdir()
        mocker.patch("src.utils.logger")
        
        # Act
        result = validate_input_file(test_dir)
        
        # Assert
        assert result is False

    def test_validate_input_file_custom_file_type(self, tmp_path, mocker):
        """
        GIVEN: un archivo válido con descripción personalizada
        WHEN: se ejecuta validate_input_file con file_type="CSV File"
        THEN: retorna True (el parámetro se usa en logging)
        """
        # Arrange
        test_file = tmp_path / "data.csv"
        test_file.write_text("data")
        mock_logger = mocker.patch("src.utils.logger")
        
        # Act
        result = validate_input_file(test_file, file_type="CSV File")
        
        # Assert
        assert result is True
        # Verifica que el logger fue llamado
        assert mock_logger.debug.called


class TestValidateInputDirectory:
    """Tests para validate_input_directory."""

    def test_validate_input_directory_success(self, tmp_path, mocker):
        """
        GIVEN: un directorio que existe
        WHEN: se ejecuta validate_input_directory
        THEN: retorna True
        """
        # Arrange
        test_dir = tmp_path / "data"
        test_dir.mkdir()
        mocker.patch("src.utils.logger")
        
        # Act
        result = validate_input_directory(test_dir)
        
        # Assert
        assert result is True

    def test_validate_input_directory_not_exists(self, tmp_path, mocker):
        """
        GIVEN: un directorio que no existe
        WHEN: se ejecuta validate_input_directory
        THEN: retorna False
        """
        # Arrange
        nonexistent_dir = tmp_path / "nonexistent"
        mocker.patch("src.utils.logger")
        
        # Act
        result = validate_input_directory(nonexistent_dir)
        
        # Assert
        assert result is False

    def test_validate_input_directory_is_file(self, tmp_path, mocker):
        """
        GIVEN: una ruta que es un archivo, no un directorio
        WHEN: se ejecuta validate_input_directory
        THEN: retorna False
        """
        # Arrange
        test_file = tmp_path / "file.txt"
        test_file.write_text("content")
        mocker.patch("src.utils.logger")
        
        # Act
        result = validate_input_directory(test_file)
        
        # Assert
        assert result is False

    def test_validate_input_directory_with_must_contain_success(self, tmp_path, mocker):
        """
        GIVEN: un directorio que contiene archivos con patrón específico
        WHEN: se ejecuta validate_input_directory con must_contain="*.json"
        THEN: retorna True
        """
        # Arrange
        test_dir = tmp_path / "data"
        test_dir.mkdir()
        (test_dir / "file1.json").write_text("{}")
        (test_dir / "file2.json").write_text("{}")
        mocker.patch("src.utils.logger")
        
        # Act
        result = validate_input_directory(test_dir, must_contain="*.json")
        
        # Assert
        assert result is True

    def test_validate_input_directory_with_must_contain_empty(self, tmp_path, mocker):
        """
        GIVEN: un directorio sin archivos que coincidan con el patrón
        WHEN: se ejecuta validate_input_directory con must_contain="*.json"
        THEN: retorna False
        """
        # Arrange
        test_dir = tmp_path / "data"
        test_dir.mkdir()
        (test_dir / "file.txt").write_text("content")
        mocker.patch("src.utils.logger")
        
        # Act
        result = validate_input_directory(test_dir, must_contain="*.json")
        
        # Assert
        assert result is False


class TestEnsureOutputDirectory:
    """Tests para ensure_output_directory."""

    def test_ensure_output_directory_creates(self, tmp_path, mocker):
        """
        GIVEN: un directorio que no existe
        WHEN: se ejecuta ensure_output_directory
        THEN: crea el directorio y retorna True
        """
        # Arrange
        output_dir = tmp_path / "output" / "nested"
        mocker.patch("src.utils.logger")
        
        # Act
        result = ensure_output_directory(output_dir)
        
        # Assert
        assert result is True
        assert output_dir.exists()
        assert output_dir.is_dir()

    def test_ensure_output_directory_already_exists(self, tmp_path, mocker):
        """
        GIVEN: un directorio que ya existe
        WHEN: se ejecuta ensure_output_directory
        THEN: retorna True sin errores
        """
        # Arrange
        output_dir = tmp_path / "existing"
        output_dir.mkdir()
        mocker.patch("src.utils.logger")
        
        # Act
        result = ensure_output_directory(output_dir)
        
        # Assert
        assert result is True
        assert output_dir.exists()

    def test_ensure_output_directory_nested_paths(self, tmp_path, mocker):
        """
        GIVEN: una ruta con múltiples niveles de anidación
        WHEN: se ejecuta ensure_output_directory
        THEN: crea todos los directorios intermedios
        """
        # Arrange
        nested_dir = tmp_path / "level1" / "level2" / "level3" / "output"
        mocker.patch("src.utils.logger")
        
        # Act
        result = ensure_output_directory(nested_dir)
        
        # Assert
        assert result is True
        assert nested_dir.exists()
        assert nested_dir.parent.exists()
        assert nested_dir.parent.parent.exists()

    def test_ensure_output_directory_permission_error(self, mocker):
        """
        GIVEN: una ruta donde no hay permisos de creación
        WHEN: se ejecuta ensure_output_directory
        THEN: retorna False y registra el error
        """
        # Arrange
        mock_logger = mocker.patch("src.utils.logger")
        problematic_path = Path("/root/cannot_create_here/output")
        mocker.patch.object(
            Path,
            "mkdir",
            side_effect=PermissionError("Permission denied")
        )
        
        # Act
        result = ensure_output_directory(problematic_path)
        
        # Assert
        assert result is False
        assert mock_logger.error.called
