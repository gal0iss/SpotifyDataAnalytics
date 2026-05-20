"""
Tests para src.validate (resumen y comprobaciones sobre dim_location_enriched).
"""

import pandas as pd
import pytest

from src.validate import validate_data, _main_cli as validate_main_cli


def _write_enriched(path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)


class TestValidateData:
    def test_read_parquet_error_logs_and_returns(self, mocker, tmp_path):
        path = tmp_path / "dim_location_enriched.parquet"
        path.write_bytes(b"not a parquet file")
        mocker.patch("src.validate.DIM_LOCATION_ENRICHED_FILE", path)
        mock_log = mocker.patch("src.validate.logger")

        validate_data()

        mock_log.error.assert_called()
        assert mock_log.error.call_args[0][0].startswith("Error leyendo archivo")

    def test_object_dtype_uses_string_unknown_rule(self, mocker, tmp_path):
        """Rama df[col].dtype == object al contar != 'Unknown' (sin roundtrip parquet)."""
        path = tmp_path / "dim_location_enriched.parquet"
        path.touch()
        df = pd.DataFrame({
            "ip_addr": ["203.0.113.1"],
            "city": pd.Series(["Madrid"], dtype=object),
            "region": pd.Series(["R"], dtype=object),
            "isp": pd.Series(["ISP"], dtype=object),
            "latitude": [0.0],
        })
        mocker.patch("src.validate.DIM_LOCATION_ENRICHED_FILE", path)
        mocker.patch("src.validate.pd.read_parquet", return_value=df)
        mock_log = mocker.patch("src.validate.logger")

        validate_data()

        mock_log.info.assert_called()
        logged = " ".join(str(c) for c in mock_log.info.call_args_list)
        assert "City: 1 registros encontrados" in logged

    def test_happy_path_logs_summary_top_and_sample(self, mocker, tmp_path):
        path = tmp_path / "out" / "dim_location_enriched.parquet"
        df = pd.DataFrame({
            "ip_addr": ["203.0.113.1", "203.0.113.2"],
            "city": ["Madrid", "Madrid"],
            "region": ["MD", "MD"],
            "isp": ["ISP_A", "ISP_B"],
            "latitude": [40.0, 41.0],
        })
        _write_enriched(path, df)
        mocker.patch("src.validate.DIM_LOCATION_ENRICHED_FILE", path)
        mock_log = mocker.patch("src.validate.logger")

        validate_data()

        mock_log.info.assert_called()
        logged = " ".join(str(c) for c in mock_log.info.call_args_list)
        assert "RESUMEN DE ENRIQUECIMIENTO" in logged
        assert "TOP 10 COMPAÑÍAS" in logged
        assert "TOP 10 CIUDADES" in logged
        assert "MUESTRA DE DATOS" in logged

    def test_missing_column_logs_warning(self, mocker, tmp_path):
        path = tmp_path / "dim_location_enriched.parquet"
        df = pd.DataFrame({
            "ip_addr": ["203.0.113.1"],
            "region": ["X"],
            "isp": ["Y"],
            "latitude": [0.0],
        })
        _write_enriched(path, df)
        mocker.patch("src.validate.DIM_LOCATION_ENRICHED_FILE", path)
        mock_log = mocker.patch("src.validate.logger")

        validate_data()

        warns = [str(c) for c in mock_log.warning.call_args_list]
        assert any("Columna 'city' no encontrada" in w for w in warns)

    def test_counting_column_exception_logs_warning(self, mocker, tmp_path):
        path = tmp_path / "dim_location_enriched.parquet"
        df = pd.DataFrame({
            "city": ["Madrid"],
            "region": ["R"],
            "isp": ["I"],
            "latitude": [1.0],
        })
        _write_enriched(path, df)
        mocker.patch("src.validate.DIM_LOCATION_ENRICHED_FILE", path)
        mock_log = mocker.patch("src.validate.logger")

        orig_getitem = pd.DataFrame.__getitem__

        def wrapped(self, key):
            if key == "city":
                raise RuntimeError("simulated failure")
            return orig_getitem(self, key)

        mocker.patch.object(pd.DataFrame, "__getitem__", wrapped)
        validate_data()

        warns = [str(c) for c in mock_log.warning.call_args_list]
        assert any("Error contando city" in w for w in warns)

    def test_isp_top_exception_logs_warning(self, mocker, tmp_path):
        path = tmp_path / "dim_location_enriched.parquet"
        df = pd.DataFrame({
            "city": ["Madrid"],
            "region": ["R"],
            "isp": ["ISP1"],
            "latitude": [1.0],
        })
        _write_enriched(path, df)
        mocker.patch("src.validate.DIM_LOCATION_ENRICHED_FILE", path)
        mock_log = mocker.patch("src.validate.logger")

        orig_vc = pd.Series.value_counts

        def vc(self, *args, **kwargs):
            if getattr(self, "name", None) == "isp":
                raise RuntimeError("isp top fail")
            return orig_vc(self, *args, **kwargs)

        mocker.patch("pandas.Series.value_counts", vc)
        validate_data()

        warns = [str(c) for c in mock_log.warning.call_args_list]
        assert any("Error procesando ISPs" in w for w in warns)

    def test_city_top_exception_logs_warning(self, mocker, tmp_path):
        path = tmp_path / "dim_location_enriched.parquet"
        df = pd.DataFrame({
            "city": ["Madrid"],
            "region": ["R"],
            "isp": ["ISP1"],
            "latitude": [1.0],
        })
        _write_enriched(path, df)
        mocker.patch("src.validate.DIM_LOCATION_ENRICHED_FILE", path)
        mock_log = mocker.patch("src.validate.logger")

        orig_vc = pd.Series.value_counts

        def vc(self, *args, **kwargs):
            if getattr(self, "name", None) == "city":
                raise RuntimeError("city top fail")
            return orig_vc(self, *args, **kwargs)

        mocker.patch("pandas.Series.value_counts", vc)
        validate_data()

        warns = [str(c) for c in mock_log.warning.call_args_list]
        assert any("Error procesando ciudades" in w for w in warns)

    def test_sample_to_string_exception_logs_warning(self, mocker, tmp_path):
        path = tmp_path / "dim_location_enriched.parquet"
        df = pd.DataFrame({
            "ip_addr": ["203.0.113.1"],
            "city": ["Madrid"],
            "region": ["R"],
            "isp": ["I"],
            "latitude": [1.0],
        })
        _write_enriched(path, df)
        mocker.patch("src.validate.DIM_LOCATION_ENRICHED_FILE", path)
        mock_log = mocker.patch("src.validate.logger")

        mocker.patch(
            "pandas.DataFrame.to_string",
            side_effect=RuntimeError("no string"),
        )
        validate_data()

        warns = [str(c) for c in mock_log.warning.call_args_list]
        assert any("Error mostrando muestra" in w for w in warns)

    def test_main_cli_logs_exception(self, mocker):
        """Punto de entrada CLI ante error en validate_data."""
        mocker.patch(
            "src.validate.validate_data", side_effect=RuntimeError("cli fail")
        )
        mock_log = mocker.patch("src.validate.logger")
        validate_main_cli()
        mock_log.error.assert_called()
