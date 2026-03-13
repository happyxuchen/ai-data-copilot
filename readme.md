# AI Data Copilot

## Introduction

AI Data Copilot is a lightweight end-to-end data platform that demonstrates how modern data engineering workflows integrate data pipelines, transformation, validation, and AI-assisted analytics.

This project simulates a real-world analytics pipeline where raw CSV data is ingested, validated, transformed into a warehouse model, profiled for data quality, and finally queried using natural language through an AI-powered SQL assistant.

The platform combines modern data engineering tools such as **Apache Airflow**, **dbt**, and **DuckDB** with an **AI SQL Copilot** and an interactive **Streamlit dashboard**, enabling users to explore datasets and generate insights using natural language queries.

The system demonstrates how a modern data platform can integrate:

- Data ingestion pipelines
- Automated data validation
- SQL transformation workflows
- Analytical warehouse modeling
- AI-driven data exploration
- Interactive analytics dashboards

---

## Project Architecture

CSV Data
↓
Validation (Great Expectations)
↓
dbt Transformation
↓
DuckDB Data Warehouse
↓
Data Profiling
↓
AI SQL Copilot
↓
Streamlit Analytics Dashboard

---

## Project Workflow

The pipeline processes data through several stages:

### 1. Data Ingestion

CSV files are uploaded or placed into the pipeline.

### 2. Data Validation

Data quality checks are performed before transformation.

### 3. Data Transformation

dbt models transform raw data into analytical tables.

### 4. Data Warehouse

DuckDB stores the transformed tables.

### 5. Data Profiling

Basic statistics such as row counts, duplicates, and schema are generated.

### 6. AI SQL Querying

Users ask questions in natural language and the system automatically generates SQL queries.

### 7. Interactive Dashboard

Streamlit allows users to explore data and run AI queries interactively.

---

## Technology Stack

| Layer           | Technology         |
| --------------- | ------------------ |
| Language        | Python             |
| Workflow        | Apache Airflow     |
| Transformation  | dbt                |
| Data Warehouse  | DuckDB             |
| Data Validation | Great Expectations |
| AI Query Engine | OpenAI / LLM       |
| Dashboard       | Streamlit          |
| Data Processing | Pandas             |
| Version Control | Git / GitHub       |

---

## Project Structure

ai-data-copilot
│
├── airflow
│ └── dags
│ └── pipeline_dag.py
│
├── app
│ └── services
│ ├── ai_sql_analyst.py
│ └── ai_analyst.py
│
├── dbt_project
│ └── my_dbt_project
│
├── streamlit_app.py
├── requirements.txt
├── README.md
└── .gitignore

---

## Features

### Natural Language Data Query

Users can ask questions such as:

Which date has the highest revenue?

The system automatically:

1. Converts the question into SQL
2. Executes the query on DuckDB
3. Returns the result and explanation

---

### Data Profiling

The dashboard automatically displays:

- Number of rows
- Number of columns
- Duplicate rows
- Missing values
- Column data types

---

### Interactive Dashboard

The Streamlit UI allows users to:

- Upload CSV files
- Run the data pipeline
- Explore warehouse tables
- Query data using AI

---

## How to Run the Project

### 1. Clone Repository

```bash
git clone https://github.com/happyxuchen/ai-data-copilot.git
cd ai-data-copilot

2. Create Virtual Environment

python -m venv .venv
source .venv/bin/activate

3. Install Dependencies

pip install -r requirements.txt

4. Run the Dashboard

streamlit run streamlit_app.py

Open your browser and visit:

http://localhost:8501

```
