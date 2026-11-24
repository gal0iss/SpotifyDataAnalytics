"""
Tests de integración para el pipeline ETL completo.

Verifica:
- Flujo end-to-end del pipeline
- Coordinación entre módulos
- Manejo de errores a nivel de pipeline

NOTA: Se parchean las constantes Path que `ReadProcess` importa desde `config`
(ya enlazadas en `src.ReadProcess`), no `config` a distancia.
"""

import json

import pandas as pd
import pytest


from src.ReadProcess import main as etl_main
from src.validate import validate_data


def _patch_etl_paths(mocker, temp_data_dir):
    """Redirige extract y salidas Parquet al directorio temporal de prueba."""
    proc = temp_data_dir["processed"]
    mocker.patch("src.ReadProcess.RAW_DATA_DIR", temp_data_dir["raw"])
    mocker.patch("src.ReadProcess.PROCESSED_DATA_DIR", proc)
    mocker.patch("src.ReadProcess.DIM_DATE_FILE", proc / "dim_date.parquet")
    mocker.patch("src.ReadProcess.DIM_DEVICE_FILE", proc / "dim_device.parquet")
    mocker.patch("src.ReadProcess.DIM_TRACK_FILE", proc / "dim_track.parquet")
    mocker.patch("src.ReadProcess.DIM_EPISODE_FILE", proc / "dim_episode.parquet")
    mocker.patch("src.ReadProcess.DIM_LOCATION_FILE", proc / "dim_location.parquet")
    mocker.patch("src.ReadProcess.FACT_TABLE_FILE", proc / "fact_table.parquet")


class TestETLPipeline:
    """Tests de integración del pipeline ETL."""

    def test_etl_pipeline_end_to_end(self, mocker, temp_data_dir, sample_streaming_data):
        """
        GIVEN: archivos JSON válidos en raw
        WHEN: se ejecuta el pipeline ETL
        THEN: produce todas las tablas parquet esperadas
        """
        json_file = temp_data_dir["raw"] / "streaming_history_0.json"
        json_file.write_text(
            sample_streaming_data.to_json(orient="records", date_format="iso")
        )

        _patch_etl_paths(mocker, temp_data_dir)
        mocker.patch("src.ReadProcess.logger")

        etl_main()

        expected_files = [
            "dim_date.parquet",
            "dim_device.parquet",
            "dim_track.parquet",
            "dim_episode.parquet",
            "dim_location.parquet",
            "fact_table.parquet",
        ]
        for name in expected_files:
            assert (temp_data_dir["processed"] / name).exists()

    def test_etl_pipeline_handles_empty_input(self, mocker, temp_data_dir):
        """
        GIVEN: directorio raw vacío
        WHEN: se ejecuta el pipeline
        THEN: lanza excepción controlada
        """
        _patch_etl_paths(mocker, temp_data_dir)
        mocker.patch("src.ReadProcess.logger")

        with pytest.raises(FileNotFoundError):
            etl_main()

    def test_etl_pipeline_processed_data_structure(self, mocker, temp_data_dir, sample_streaming_data):
        """
        GIVEN: datos de entrada válidos
        WHEN: se ejecuta el pipeline
        THEN: la fact_table contiene las columnas y relaciones correctas
        """
        json_file = temp_data_dir["raw"] / "streaming_history_0.json"
        json_file.write_text(
            sample_streaming_data.to_json(orient="records", date_format="iso")
        )

        _patch_etl_paths(mocker, temp_data_dir)
        mocker.patch("src.ReadProcess.logger")

        etl_main()

        fact_table = pd.read_parquet(temp_data_dir["processed"] / "fact_table.parquet")

        required_columns = [
            "event_id", "date_id", "track_id", "episode_id",
            "device_id", "location_id", "ms_played",
        ]
        assert all(col in fact_table.columns for col in required_columns)

        assert fact_table["track_id"].notna().all()
        assert fact_table["device_id"].notna().all()
        assert fact_table["location_id"].notna().all()

    def test_etl_pipeline_preserves_data_count(self, mocker, temp_data_dir, sample_streaming_data):
        """
        GIVEN: datos con N registros
        WHEN: se ejecuta el pipeline
        THEN: fact_table contiene N registros (sin pérdidas)
        """
        initial_count = len(sample_streaming_data)
        json_file = temp_data_dir["raw"] / "streaming_history_0.json"
        json_file.write_text(
            sample_streaming_data.to_json(orient="records", date_format="iso")
        )

        _patch_etl_paths(mocker, temp_data_dir)
        mocker.patch("src.ReadProcess.logger")

        etl_main()

        fact_table = pd.read_parquet(temp_data_dir["processed"] / "fact_table.parquet")
        assert len(fact_table) == initial_count

    @pytest.mark.parametrize("all_missing", [False, True])
    def test_reserved_and_missing_keys_preserve_events(
        self, mocker, temp_data_dir, sample_streaming_data, all_missing
    ):
        data = sample_streaming_data.copy()
        data["platform"] = ["Unknown", "Windows", None]
        data["ip_addr"] = ["0.0.0.0", "203.0.113.2", None]
        if all_missing:
            data[["platform", "ip_addr"]] = None
        (temp_data_dir["raw"] / "reserved.json").write_text(
            data.to_json(orient="records", date_format="iso"), encoding="utf-8"
        )
        _patch_etl_paths(mocker, temp_data_dir)
        mocker.patch("src.ReadProcess.logger")

        etl_main()

        proc = temp_data_dir["processed"]
        fact = pd.read_parquet(proc / "fact_table.parquet")
        assert len(fact) == len(data)
        assert fact["event_id"].is_unique
        assert fact["ms_played"].sum() == data["ms_played"].sum()
        for table, natural_key, foreign_key in (
            ("dim_device", "platform", "device_id"),
            ("dim_location", "ip_addr", "location_id"),
        ):
            dimension = pd.read_parquet(proc / f"{table}.parquet")
            assert dimension[natural_key].is_unique
            assert (dimension[foreign_key] == -1).sum() == 1
            assert fact[foreign_key].isin(dimension[foreign_key]).all()
            assert (fact.loc[fact["event_id"].isin([0, 2]), foreign_key] == -1).all()
            if all_missing:
                assert (fact[foreign_key] == -1).all()

    def test_duplicate_dimension_key_fails_before_writing(
        self, mocker, temp_data_dir, sample_streaming_data
    ):
        from src.ReadProcess import create_dim_device

        (temp_data_dir["raw"] / "streaming.json").write_text(
            sample_streaming_data.to_json(orient="records", date_format="iso"),
            encoding="utf-8",
        )
        dimension = create_dim_device(sample_streaming_data)
        duplicate = pd.concat([dimension, dimension.iloc[[1]]], ignore_index=True)
        _patch_etl_paths(mocker, temp_data_dir)
        mocker.patch("src.ReadProcess.create_dim_device", return_value=duplicate)
        mocker.patch("src.ReadProcess.logger")

        with pytest.raises(pd.errors.MergeError):
            etl_main()

        assert not list(temp_data_dir["processed"].glob("*.parquet"))


class TestValidateData:
    """Tests para la validación de datos."""

    def test_validate_data_with_enriched_file(self, mocker, temp_data_dir, sample_dim_location):
        """
        GIVEN: archivo de ubicación enriquecida existe
        WHEN: se ejecuta validate_data
        THEN: ejecuta validaciones sin error
        """
        enriched = sample_dim_location.copy()
        enriched["city"] = ["Madrid", "New York", "London", "Unknown"]
        enriched["region"] = ["Madrid", "NY", "London", "Unknown"]
        enriched["latitude"] = [40.4, 40.7, 51.5, 0]
        enriched["longitude"] = [-3.7, -74.0, -0.1, 0]
        enriched["isp"] = ["Telefonica", "Verizon", "Vodafone", "Unknown"]

        output_file = temp_data_dir["processed"] / "dim_location_enriched.parquet"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        enriched.to_parquet(output_file)

        mocker.patch("src.validate.DIM_LOCATION_ENRICHED_FILE", output_file)
        mock_logger = mocker.patch("src.validate.logger")

        validate_data()

        assert mock_logger.info.called

    def test_validate_data_missing_file(self, mocker, temp_data_dir):
        """
        GIVEN: archivo enriquecido no existe
        WHEN: se ejecuta validate_data
        THEN: loguea advertencia y retorna
        """
        missing = temp_data_dir["processed"] / "dim_location_enriched.parquet"
        mocker.patch("src.validate.DIM_LOCATION_ENRICHED_FILE", missing)
        mock_logger = mocker.patch("src.validate.logger")

        validate_data()

        assert mock_logger.warning.called


class TestErrorHandling:
    """Tests para el manejo de errores en el pipeline."""

    def test_malformed_json_file(self, mocker, temp_data_dir):
        """
        GIVEN: archivo JSON malformado
        WHEN: se intenta procesar
        THEN: lanza excepción al leer
        """
        json_file = temp_data_dir["raw"] / "malformed.json"
        json_file.write_text("{ invalid json content")

        _patch_etl_paths(mocker, temp_data_dir)
        mocker.patch("src.ReadProcess.logger")

        with pytest.raises(Exception):
            etl_main()

    def test_missing_required_columns(self, mocker, temp_data_dir):
        """
        GIVEN: datos JSON sin columnas requeridas por el ETL
        WHEN: se procesa
        THEN: falla con KeyError (columnas como ts ausentes)
        """
        incomplete_data = [{"incomplete": "data"}]
        json_file = temp_data_dir["raw"] / "incomplete.json"
        json_file.write_text(json.dumps(incomplete_data))

        _patch_etl_paths(mocker, temp_data_dir)
        mocker.patch("src.ReadProcess.logger")

        with pytest.raises(KeyError):
            etl_main()
