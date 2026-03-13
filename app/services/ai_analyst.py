import duckdb
from openai import OpenAI

client = OpenAI()

DB_PATH = "dbt_project/data/warehouse/pipeline.duckdb"

def ask_ai(question: str):
    conn = duckdb.connect(DB_PATH)

    df = conn.execute("""
        SELECT * FROM menu_summary
        LIMIT 100
    """).df()

    data_preview = df.to_markdown(index=False)

    prompt = f"""
You are a senior data analyst.

Here is a preview of a dataset:

{data_preview}

User question:
{question}

Provide a clear analytical answer based only on the dataset preview.
""".strip()

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    conn.close()
    return response.choices[0].message.content


if __name__ == "__main__":
    while True:
        question = input("\nAsk a data question: ")
        answer = ask_ai(question)
        print("\nAI Analysis:\n")
        print(answer)