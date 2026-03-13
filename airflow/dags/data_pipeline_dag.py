from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = "/Users/guoxuchen/ai-data-copilot/ai-data-copilot"
DBT_PROJECT_DIR = f"{PROJECT_DIR}/dbt_project/my_dbt_project"
VENV_PYTHON = f"{PROJECT_DIR}/.venv/bin/python"
VENV_DBT = f"{PROJECT_DIR}/.venv/bin/dbt"

with DAG(
    dag_id="data_pipeline_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["data_pipeline", "ai_data_copilot"],
) as dag:

    validate_csv = BashOperator(
        task_id="validate_csv",
        bash_command=f"cd {PROJECT_DIR} && {VENV_PYTHON} app/services/run_validation.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && {VENV_DBT} run --profiles-dir ..",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && {VENV_DBT} test --profiles-dir ..",
    )

    profile_data = BashOperator(
        task_id="profile_data",
        bash_command=f"cd {PROJECT_DIR} && {VENV_PYTHON} app/services/profiler.py",
    )

    ai_summary = BashOperator(
        task_id="ai_summary",
        bash_command=f"cd {PROJECT_DIR} && {VENV_PYTHON} app/services/llm_summary.py",
    )

    validate_csv >> dbt_run >> dbt_test >> profile_data >> ai_summary