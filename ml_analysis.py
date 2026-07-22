from pathlib import Path
import pandas as pd
from sklearn.preprocessing import LabelEncoder

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "processedPedro"

DIM_FILES = {
    "dim_date": "dim_date.parquet",
    "dim_device": "dim_device.parquet",
    "dim_episode": "dim_episode.parquet",
    "dim_location": "dim_location.parquet",
    "dim_location_enriched": "dim_location_enriched.parquet",
    "dim_track": "dim_track.parquet",
}


def load_dimensions(data_dir: Path):
    """Carga cada archivo dim_*.parquet de processedPedro con su nombre referencial."""
    dataframes = {}
    for name, filename in DIM_FILES.items():
        path = data_dir / filename
        if not path.exists():
            print(f"WARN: archivo no encontrado: {path}")
            continue
        df = pd.read_parquet(path)
        dataframes[name] = df
    return dataframes


def analyze_dataframe(name: str, df: pd.DataFrame):
    print("\n" + "=" * 80)
    print(f"Análisis exploratorio de: {name}")
    print("=" * 80)
    print(f"Ruta: {DATA_DIR / DIM_FILES[name]}")
    print(f"Shape: {df.shape[0]} filas x {df.shape[1]} columnas")
    print("Columns:", list(df.columns))
    print("Dtypes:")
    print(df.dtypes)
    print("\nPrimeras 10 filas:")
    print(df.head(10).to_string(index=False))
    print("\nConteo de valores nulos por columna:")
    print(df.isna().sum())
    print("\nDescripción estadística:")
    try:
        print(df.describe(include='all', datetime_is_numeric=True).transpose())
    except TypeError:
        print(df.describe(include='all').transpose())

    categorical_columns = df.select_dtypes(include=["object", "string", "category"]).columns.tolist()
    if categorical_columns:
        print("\nColumnas categóricas detectadas:", categorical_columns)
        for col in categorical_columns:
            n_unique = df[col].nunique(dropna=False)
            print(f"- {col}: {n_unique} valores únicos")
            sample_values = df[col].dropna().astype(str).drop_duplicates().head(5).tolist()
            print(f"  Muestra: {sample_values}")
            if n_unique <= 20:
                encoder = LabelEncoder()
                encoded = encoder.fit_transform(df[col].fillna("__MISSING__").astype(str))
                print(f"  Valores codificados (primeros 5): {encoded[:5].tolist()}")
    else:
        print("\nNo se detectaron columnas categóricas para codificar con scikit-learn.")


if __name__ == "__main__":
    print("Cargando datos desde:", DATA_DIR)
    dimensions = load_dimensions(DATA_DIR)
    if not dimensions:
        raise SystemExit("No se cargó ninguna dimensión. Verifique que data/processedPedro exista y contenga archivos dim_*.parquet.")

    for dim_name, df in dimensions.items():
        analyze_dataframe(dim_name, df)

    print("\nAnálisis exploratorio completado.")
