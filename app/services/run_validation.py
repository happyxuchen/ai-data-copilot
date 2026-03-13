from validation import validate_csv

if __name__ == "__main__":
    file_path = "dbt_project/data/raw/menu.csv"

    print("Starting validation...")

    result = validate_csv(file_path)

    print("Validation finished.")
    print(result)