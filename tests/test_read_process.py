"""
Tests para el módulo ReadProcess (Extract & Transform).

Verifica:
- Extracción de archivos JSON
- Limpieza de datos
- Creación de dimensiones (dim_date, dim_device, etc.)
- Creación de tabla de hechos
- Persistencia en Parquet
"""

import pytest
import pandas as pd
import numpy as np
import json

from src.ReadProcess import (
    extract_json_files,
    clean_data,
    create_dim_date,
    create_dim_device,
    create_dim_track,
    create_dim_episode,
    create_dim_location,
    create_fact_table,
    save_to_parquet,
)
class TestExtractJsonFiles:
    """Tests para la funcionalidad de extracción de JSONs."""

    def test_extract_json_files_success(self, mocker, temp_data_dir, sample_streaming_data):
        """
        GIVEN: archivos JSON válidos en raw_dir
        WHEN: se ejecuta extract_json_files
        THEN: retorna DataFrame con datos combinados
        """
        # Arrange
        json_content = sample_streaming_data.to_json(orient="records", date_format="iso")
        json_file = temp_data_dir["raw"] / "streaming_history_0.json"
        json_file.write_text(json_content)
        
        # Act
        result = extract_json_files(temp_data_dir["raw"])
        
        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert "ts" in result.columns
        assert pd.api.types.is_datetime64_any_dtype(result["ts"])

    def test_extract_json_files_multiple_files(self, mocker, temp_data_dir):
        """
        GIVEN: múltiples archivos JSON en raw_dir
        WHEN: se ejecuta extract_json_files
        THEN: concatena todos los archivos en un solo DataFrame
        """
        # Arrange: crear 3 archivos JSON
        for i in range(3):
            data = [{"col": i, "value": f"row_{i}"}]
            json_file = temp_data_dir["raw"] / f"file_{i}.json"
            json_file.write_text(json.dumps(data))
        
        # Act
        result = extract_json_files(temp_data_dir["raw"])
        
        # Assert
        assert len(result) == 3
        assert set(result["col"].values) == {0, 1, 2}

    def test_extract_json_files_path_not_exists(self, mocker, temp_data_dir):
        """
        GIVEN: ruta que no existe
        WHEN: se ejecuta extract_json_files
        THEN: lanza FileNotFoundError
        """
        # Arrange
        invalid_path = temp_data_dir["root"] / "nonexistent"
        mocker.patch("src.ReadProcess.logger")
        
        # Act & Assert
        with pytest.raises(FileNotFoundError):
            extract_json_files(invalid_path)

    def test_extract_json_files_no_json_files(self, mocker, temp_data_dir):
        """
        GIVEN: directorio sin archivos JSON
        WHEN: se ejecuta extract_json_files
        THEN: lanza FileNotFoundError
        """
        # Arrange
        mocker.patch("src.ReadProcess.logger")
        
        # Act & Assert
        with pytest.raises(FileNotFoundError):
            extract_json_files(temp_data_dir["raw"])


class TestCleanData:
    """Tests para la limpieza de datos."""

    def test_clean_data_removes_empty_audiobook_columns(self, sample_streaming_data):
        """
        GIVEN: datos con columnas vacías de audiobook
        WHEN: se ejecuta clean_data
        THEN: elimina esas columnas
        """
        # Arrange
        df = sample_streaming_data.copy()
        df["audiobook_name"] = None
        df["audiobook_author"] = None
        
        # Act
        result = clean_data(df)
        
        # Assert
        assert "audiobook_name" not in result.columns
        assert "audiobook_author" not in result.columns

    def test_clean_data_adds_event_id(self, sample_streaming_data):
        """
        GIVEN: datos sin event_id
        WHEN: se ejecuta clean_data
        THEN: añade columna event_id unique
        """
        # Act
        result = clean_data(sample_streaming_data)
        
        # Assert
        assert "event_id" in result.columns
        assert len(result["event_id"].unique()) == len(result)
        assert result["event_id"].min() == 0

    def test_clean_data_resets_index(self, sample_streaming_data):
        """
        GIVEN: datos con índice personalizado
        WHEN: se ejecuta clean_data
        THEN: resetea el índice
        """
        # Arrange
        df = sample_streaming_data.copy()
        df.index = [100, 101, 102]
        
        # Act
        result = clean_data(df)
        
        # Assert
        assert list(result.index) == [0, 1, 2]


class TestDimensionCreation:
    """Tests para la creación de dimensiones."""

    def test_create_dim_date(self, sample_streaming_data):
        """
        GIVEN: datos con columna ts
        WHEN: se ejecuta create_dim_date
        THEN: crea dimensión con campos temporales
        """
        # Act
        result = create_dim_date(sample_streaming_data)
        
        # Assert
        assert isinstance(result, pd.DataFrame)
        assert set(["date_id", "year", "month", "day", "weekday", "hour"]).issubset(result.columns)
        assert len(result) == 3
        assert result["year"].iloc[0] == 2024

    def test_create_dim_device(self, sample_streaming_data):
        """
        GIVEN: datos con columna platform
        WHEN: se ejecuta create_dim_device
        THEN: crea dimensión de dispositivos clasificados
        """
        # Act
        result = create_dim_device(sample_streaming_data)
        
        # Assert
        assert "device_id" in result.columns
        assert "device_type" in result.columns
        assert set(result["device_type"].unique()).issubset(
            {"mobile", "desktop", "web", "other", "unknown"}
        )

    def test_create_dim_device_classification(self):
        """
        GIVEN: datos con diferentes tipos de platform
        WHEN: se ejecuta create_dim_device
        THEN: los tipos se clasifican correctamente
        """
        # Arrange
        df = pd.DataFrame({
            "platform": ["Android", "iOS", "Windows", "MacOS", "Web", "Unknown", None],
            "spotify_track_uri": ["uri"] * 7,
            "spotify_episode_uri": [None] * 7,
        })
        
        # Act
        result = create_dim_device(df)
        
        # Assert
        device_types = result[result["platform"].notna()]["device_type"].unique()
        assert "mobile" in device_types
        assert "desktop" in device_types
        assert "web" in device_types

    def test_create_dim_track(self, sample_streaming_data):
        """
        GIVEN: datos con información de tracks
        WHEN: se ejecuta create_dim_track
        THEN: crea dimensión de tracks sin duplicados
        """
        # Act
        result = create_dim_track(sample_streaming_data)
        
        # Assert
        assert "track_id" in result.columns
        assert "track_name" in result.columns
        assert "artist_name" in result.columns
        assert len(result) == 4  # 3 únicos + 1 unknown

    def test_create_dim_episode(self, sample_streaming_data_with_podcasts):
        """
        GIVEN: datos con episodios de podcast
        WHEN: se ejecuta create_dim_episode
        THEN: crea dimensión de episodios sin duplicados
        """
        # Act
        result = create_dim_episode(sample_streaming_data_with_podcasts)
        
        # Assert
        assert "episode_id" in result.columns
        assert "episode_name" in result.columns
        assert "show_name" in result.columns
        assert len(result) >= 1  # al menos unknown

    def test_create_dim_location(self, sample_streaming_data):
        """
        GIVEN: datos con IPs y países
        WHEN: se ejecuta create_dim_location
        THEN: crea dimensión de ubicaciones sin duplicados
        """
        # Act
        result = create_dim_location(sample_streaming_data)
        
        # Assert
        assert "location_id" in result.columns
        assert "ip_addr" in result.columns
        assert len(result) == 4  # 3 únicos + 1 unknown


class TestFactTableCreation:
    """Tests para la creación de tabla de hechos."""

    def test_create_fact_table_structure(self, sample_streaming_data):
        """
        GIVEN: datos limpios
        WHEN: se ejecuta create_fact_table
        THEN: crea tabla de hechos con estructura correcta
        """
        # Arrange
        df = sample_streaming_data.copy()
        df["event_id"] = range(len(df))
        df["track_id"] = [1, 2, 3]
        df["episode_id"] = np.nan
        df["device_id"] = [1, 2, 3]
        df["location_id"] = [1, 2, 3]
        
        # Act
        result = create_fact_table(df)
        
        # Assert
        required_cols = [
            "event_id", "date_id", "track_id", "episode_id",
            "device_id", "location_id", "ms_played", "skipped", "shuffle",
            "offline", "incognito_mode"
        ]
        assert all(col in result.columns for col in required_cols)

    def test_create_fact_table_fills_nan_ids(self, sample_streaming_data):
        """
        GIVEN: datos con track_id/episode_id NaN
        WHEN: se ejecuta create_fact_table
        THEN: rellena NaN con -1 (unknown)
        """
        # Arrange
        df = sample_streaming_data.copy()
        df["event_id"] = range(len(df))
        df["track_id"] = [1, np.nan, 3]
        df["episode_id"] = np.nan
        df["device_id"] = [1, 2, 3]
        df["location_id"] = [1, 2, 3]
        
        # Act
        result = create_fact_table(df)
        
        # Assert
        assert result["track_id"].iloc[1] == -1
        assert (result["episode_id"] == -1).all()

    def test_create_fact_table_bool_conversion(self, sample_streaming_data):
        """
        GIVEN: datos con columnas booleanas como float
        WHEN: se ejecuta create_fact_table
        THEN: convierte a boolean
        """
        # Arrange
        df = sample_streaming_data.copy()
        df["event_id"] = range(len(df))
        df["track_id"] = [1, 2, 3]
        df["episode_id"] = np.nan
        df["device_id"] = [1, 2, 3]
        df["location_id"] = [1, 2, 3]
        df["skipped"] = [0.0, 1.0, 0.0]  # como float
        df["offline"] = [0.0, 0.0, 1.0]  # como float
        
        # Act
        result = create_fact_table(df)
        
        # Assert
        assert result["skipped"].dtype == bool
        assert result["offline"].dtype == bool


class TestSaveToParquet:
    """Tests para la persistencia en Parquet."""

    def test_save_to_parquet_success(self, temp_data_dir, sample_streaming_data, mocker):
        """
        GIVEN: un DataFrame válido
        WHEN: se ejecuta save_to_parquet
        THEN: guarda el archivo sin errores
        """
        # Arrange
        mocker.patch("src.ReadProcess.logger")
        output_file = temp_data_dir["processed"] / "test_table.parquet"
        
        # Act
        save_to_parquet(sample_streaming_data, output_file)
        
        # Assert
        assert output_file.exists()

    def test_save_to_parquet_creates_directory(self, temp_data_dir, sample_streaming_data, mocker):
        """
        GIVEN: directorio de destino que no existe
        WHEN: se ejecuta save_to_parquet
        THEN: guarda el archivo en la ruta especificada
        """
        # Arrange
        mocker.patch("src.ReadProcess.logger")
        output_file = temp_data_dir["processed"] / "subdir" / "test.parquet"
        
        # Act
        save_to_parquet(sample_streaming_data, output_file)
        
        # Assert
        assert output_file.exists()
        assert output_file.parent.exists()

    def test_save_to_parquet_roundtrip(self, temp_data_dir, sample_streaming_data, mocker):
        """
        GIVEN: un DataFrame original
        WHEN: se guarda y se lee de Parquet
        THEN: los datos son idénticos (excepto tipos)
        """
        # Arrange
        mocker.patch("src.ReadProcess.logger")
        output_file = temp_data_dir["processed"] / "roundtrip.parquet"
        
        # Act
        save_to_parquet(sample_streaming_data, output_file)
        loaded = pd.read_parquet(output_file)
        
        # Assert
        pd.testing.assert_frame_equal(
            sample_streaming_data,
            loaded,
            check_dtype=False,
            check_freq=False
        )
