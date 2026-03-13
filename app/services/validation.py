import os
import pandas as pd

REQUIRED_COLUMNS = ["id", "name", "amount", "event_date"]

def validate_csv(file_path: str) -> dict:
    print(f"Starting validation for: {file_path}")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    df = pd.read_csv(file_path)

    print("Row count:", len(df))
    print("Columns:", list(df.columns))

    # 1. 文件不能为空
    if df.empty:
        raise ValueError("CSV is empty.")

    # 2. 必需列检查
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # 3. 关键列不能为空
    if df["id"].isnull().any():
        raise ValueError("Column 'id' contains null values")

    if df["name"].isnull().any():
        raise ValueError("Column 'name' contains null values")

    if df["amount"].isnull().any():
        raise ValueError("Column 'amount' contains null values")

    if df["event_date"].isnull().any():
        raise ValueError("Column 'event_date' contains null values")

    # 4. id 必须唯一
    if df["id"].duplicated().any():
        raise ValueError("Column 'id' contains duplicate values")

    # 5. amount 必须 >= 0
    if (df["amount"] < 0).any():
        raise ValueError("Column 'amount' contains negative values")

    print("Validation passed!")

    return {
        "success": True,
        "row_count": len(df),
        "columns": list(df.columns)
    }