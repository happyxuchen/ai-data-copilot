import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def generate_dataset_analysis(profile, columns):

    prompt = f"""
You are a professional data analyst.

Dataset columns:
{columns}

Dataset profile:
{profile}

Please analyze the dataset and provide:

1. What the dataset appears to represent
2. Any potential data quality issues
3. Possible analysis that could be performed

Keep the answer short and clear.
"""

    response = client.responses.create(
        model="gpt-4.1-mini",
        input=prompt
    )

    return response.output_text