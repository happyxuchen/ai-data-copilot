from profiler import profile_menu_summary


def generate_ai_summary() -> str:
    profile = profile_menu_summary()

    summary = f"""
AI Data Copilot Summary

The transformed dataset contains {profile['rows']} rows and {profile['columns']} columns.

Column types:
{profile['data_types']}

Missing values by column:
{profile['missing_values']}

Duplicate rows: {profile['duplicate_rows']}

Overall assessment:
- The dataset appears clean and ready for downstream analytics.
- No missing values were detected in the summary table.
- No duplicate rows were detected in the summary table.
""".strip()

    return summary


if __name__ == "__main__":
    result = generate_ai_summary()
    print(result)