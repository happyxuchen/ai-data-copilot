import duckdb
from openai import OpenAI

client = OpenAI()

DB_PATH = "dbt_project/data/warehouse/pipeline.duckdb"
TABLE_NAME = "menu_summary"


def generate_sql(question: str) -> str:
    prompt = f"""
You are a senior analytics engineer.

You are working with a DuckDB database table named `{TABLE_NAME}`.

The table schema is:
- event_date (date)
- record_count (integer)
- avg_amount (float)
- total_amount (float)

Write a valid DuckDB SQL query to answer the user's question.

Rules:
- Only use the table `{TABLE_NAME}`
- Return only SQL
- Do not include markdown fences
- Do not include explanations

User question:
{question}
""".strip()

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
    )

    sql = response.choices[0].message.content.strip()
    return sql


def run_sql(sql: str):
    conn = duckdb.connect(DB_PATH)
    df = conn.execute(sql).df()
    conn.close()
    return df


def explain_results(question: str, sql: str, result_markdown: str) -> str:
    prompt = f"""
You are a senior data analyst.

User question:
{question}

SQL used:
{sql}

Query results:
{result_markdown}

Provide a concise analytical explanation of the result.
""".strip()

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
    )

    return response.choices[0].message.content.strip()


def ask_ai_sql(question: str):
    sql = generate_sql(question)
    print("\nGenerated SQL:\n")
    print(sql)

    df = run_sql(sql)

    if df.empty:
        result_markdown = "No rows returned."
    else:
        result_markdown = df.to_markdown(index=False)

    print("\nQuery Results:\n")
    print(result_markdown)

    explanation = explain_results(question, sql, result_markdown)

    print("\nAI Explanation:\n")
    print(explanation)


if __name__ == "__main__":
    while True:
        question = input("\nAsk a data question (or type 'exit'): ").strip()
        if question.lower() == "exit":
            break
        ask_ai_sql(question)