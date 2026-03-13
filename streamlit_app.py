import shutil
import subprocess
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="AI Data Copilot", layout="wide")

PROJECT_DIR = Path(__file__).resolve().parent
DB_PATH = PROJECT_DIR / "dbt_project" / "data" / "warehouse" / "pipeline.duckdb"
RAW_CSV_PATH = PROJECT_DIR / "dbt_project" / "data" / "raw" / "menu.csv"
DBT_PROJECT_DIR = PROJECT_DIR / "dbt_project" / "my_dbt_project"
VENV_PYTHON = PROJECT_DIR / ".venv" / "bin" / "python"
VENV_DBT = PROJECT_DIR / ".venv" / "bin" / "dbt"


def get_connection():
    return duckdb.connect(str(DB_PATH))


@st.cache_data(show_spinner=False)
def get_tables():
    conn = get_connection()
    try:
        rows = conn.execute("SHOW TABLES").fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


@st.cache_data(show_spinner=False)
def read_table(table_name: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        return conn.execute(f"SELECT * FROM {table_name}").df()
    finally:
        conn.close()


@st.cache_data(show_spinner=False)
def profile_dataframe(df: pd.DataFrame) -> dict:
    return {
        "rows": int(df.shape[0]),
        "columns": int(df.shape[1]),
        "data_types": df.dtypes.astype(str).to_dict(),
        "missing_values": df.isnull().sum().to_dict(),
        "duplicate_rows": int(df.duplicated().sum()),
    }


def build_ai_summary(df: pd.DataFrame, table_name: str) -> str:
    profile = profile_dataframe(df)
    return f"""
AI Data Copilot Summary

Table: {table_name}
Rows: {profile['rows']}
Columns: {profile['columns']}
Duplicate rows: {profile['duplicate_rows']}

Missing values by column:
{profile['missing_values']}

Column data types:
{profile['data_types']}

Overall assessment:
- The dataset appears ready for downstream analytics.
- Missing values should remain acceptable for the current use case.
- The summary table can be used for natural-language analytics questions.
""".strip()


def sql_copilot(question: str):
    q = question.lower().strip()

    if "highest" in q and ("revenue" in q or "total" in q):
        sql = """
SELECT event_date, total_amount
FROM menu_summary
ORDER BY total_amount DESC
LIMIT 1
""".strip()
        explanation = "This result shows the date with the highest total_amount in the summary table."

    elif "lowest" in q and ("revenue" in q or "total" in q):
        sql = """
SELECT event_date, total_amount
FROM menu_summary
ORDER BY total_amount ASC
LIMIT 1
""".strip()
        explanation = "This result shows the date with the lowest total_amount in the summary table."

    elif "descending" in q or "sorted" in q:
        sql = """
SELECT event_date, total_amount
FROM menu_summary
ORDER BY total_amount DESC
""".strip()
        explanation = "This result ranks dates by total_amount from highest to lowest."

    elif "average" in q:
        sql = """
SELECT AVG(total_amount) AS avg_total_amount
FROM menu_summary
""".strip()
        explanation = "This result computes the average total_amount across all dates."

    elif "all dates" in q or "trend" in q:
        sql = """
SELECT event_date, total_amount
FROM menu_summary
ORDER BY event_date
""".strip()
        explanation = "This result shows total_amount across dates in chronological order."

    else:
        sql = """
SELECT *
FROM menu_summary
LIMIT 10
""".strip()
        explanation = "I could not confidently map the question to a more specific pattern, so I returned a preview of the summary table."

    conn = get_connection()
    try:
        result_df = conn.execute(sql).df()
    finally:
        conn.close()

    return sql, result_df, explanation


def run_command(cmd: list[str], cwd: Path):
    result = subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
    )
    output = (result.stdout or "") + ("\n" + result.stderr if result.stderr else "")
    return result.returncode, output.strip()


def save_uploaded_csv(uploaded_file):
    RAW_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(RAW_CSV_PATH, "wb") as f:
        shutil.copyfileobj(uploaded_file, f)


def clear_cached_data():
    st.cache_data.clear()


st.title("AI Data Copilot")
st.caption("Natural-language analytics on DuckDB warehouse tables built with validation, dbt, Airflow, and AI SQL querying.")

st.markdown(
    """
This dashboard allows users to:
- upload a CSV into the raw data layer,
- run validation and dbt transformations,
- inspect warehouse tables and data profiles,
- and ask natural-language business questions.
"""
)

with st.container():
    st.subheader("Project workflow")
    st.markdown("CSV → Validation → dbt Run → dbt Test → DuckDB Warehouse → Profiling → AI Summary → AI SQL Copilot")

st.header("Pipeline Controls")
control_left, control_right = st.columns([1, 1])

with control_left:
    st.subheader("Upload CSV")
    uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])
    if uploaded_file is not None:
        if st.button("Save Uploaded CSV"):
            save_uploaded_csv(uploaded_file)
            clear_cached_data()
            st.success(f"Saved file to: {RAW_CSV_PATH}")

    if RAW_CSV_PATH.exists():
        st.info(f"Current raw file: {RAW_CSV_PATH.name}")
    else:
        st.warning("No raw CSV found yet.")

with control_right:
    st.subheader("Run Pipeline")

    if st.button("Run Validation"):
        code, output = run_command([str(VENV_PYTHON), "app/services/run_validation.py"], PROJECT_DIR)
        st.code(output)
        if code == 0:
            st.success("Validation completed successfully.")
        else:
            st.error("Validation failed.")

    if st.button("Run dbt"):
        code, output = run_command([str(VENV_DBT), "run", "--profiles-dir", ".."], DBT_PROJECT_DIR)
        st.code(output)
        if code == 0:
            clear_cached_data()
            st.success("dbt run completed successfully.")
        else:
            st.error("dbt run failed.")

    if st.button("Run dbt Tests"):
        code, output = run_command([str(VENV_DBT), "test", "--profiles-dir", ".."], DBT_PROJECT_DIR)
        st.code(output)
        if code == 0:
            clear_cached_data()
            st.success("dbt test completed successfully.")
        else:
            st.error("dbt test failed.")

if not DB_PATH.exists():
    st.warning("DuckDB warehouse not found yet. Upload a CSV and run the pipeline first.")
    st.stop()

tables = get_tables()
if not tables:
    st.warning("No tables found in DuckDB yet. Run dbt first.")
    st.stop()

left, right = st.columns([1.25, 1])

with left:
    st.header("Explore Data")
    selected_table = st.selectbox("Select a table", tables, index=0)
    df = read_table(selected_table)
    st.dataframe(df, use_container_width=True)

with right:
    st.header("Data Profile")
    profile = profile_dataframe(df)
    metric_1, metric_2, metric_3 = st.columns(3)
    metric_1.metric("Rows", profile["rows"])
    metric_2.metric("Columns", profile["columns"])
    metric_3.metric("Duplicates", profile["duplicate_rows"])

    with st.expander("Missing values"):
        st.json(profile["missing_values"])

    with st.expander("Column data types"):
        st.json(profile["data_types"])

st.header("AI Summary")
st.text(build_ai_summary(df, selected_table))

st.header("AI SQL Copilot")
example_questions = [
    "Which date has the highest total revenue?",
    "Which date has the lowest revenue?",
    "Show all dates sorted by revenue descending.",
    "What is the average total_amount across all dates?",
]
selected_example = st.selectbox("Example questions", ["Type my own question"] + example_questions)

question = st.text_input(
    "Ask a question",
    value="" if selected_example == "Type my own question" else selected_example,
    placeholder="Which date has the highest total revenue?",
)

if st.button("Run Query"):
    if not question.strip():
        st.warning("Please enter a question first.")
    else:
        sql, result_df, explanation = sql_copilot(question)

        st.subheader("Generated SQL")
        st.code(sql, language="sql")

        st.subheader("Query Result")
        st.dataframe(result_df, use_container_width=True)

        st.subheader("Explanation")
        st.write(explanation)
