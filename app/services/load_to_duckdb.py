import pandas as pd
import duckdb
from pathlib import Path

DB_PATH = "data/warehouse/pipeline.duckdb"

def load_csv_to_duckdb(file_path: str, table_name: str = "raw_uploaded_data") -> dict:
    Path("data/warehouse").mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(file_path)
    conn = duckdb.connect(DB_PATH)

    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.register("temp_df", df)
    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")

    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    conn.close()

    return {
        "table_name": table_name,
        "row_count": row_count,
        "db_path": DB_PATH
    }