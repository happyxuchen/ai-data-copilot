import pandas as pd
import duckdb
from pathlib import Path


def profile_dataframe(df: pd.DataFrame):
    profile = {}

    profile["rows"] = df.shape[0]
    profile["columns"] = df.shape[1]
    profile["data_types"] = df.dtypes.astype(str).to_dict()
    profile["missing_values"] = df.isnull().sum().to_dict()
    profile["duplicate_rows"] = int(df.duplicated().sum())

    return profile


def profile_menu_summary():
    project_root = Path(__file__).resolve().parents[2]

    db_path = project_root / "dbt_project" / "data" / "warehouse" / "pipeline.duckdb"

    con = duckdb.connect(str(db_path))

    df = con.execute("select * from menu_summary").fetchdf()

    con.close()

    profile = profile_dataframe(df)

    print("Data profile:")
    print(profile)

    return profile


if __name__ == "__main__":
    profile_menu_summary()