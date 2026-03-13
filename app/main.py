from fastapi import FastAPI, UploadFile, File
import pandas as pd
from io import StringIO

from app.services.profiler import profile_dataframe
from app.services.llm_service import generate_dataset_analysis


app = FastAPI(title="AI Data Copilot")


@app.get("/")
def read_root():
    return {"message": "AI Data Copilot is running"}


@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        return {"error": "Only CSV files are supported."}

    contents = await file.read()
    csv_string = contents.decode("utf-8")
    df = pd.read_csv(StringIO(csv_string))

    profile = profile_dataframe(df)

    return {
        "filename": file.filename,
        "column_names": df.columns.tolist(),
        "profile": profile
    }


@app.post("/analyze")
async def analyze_csv(file: UploadFile = File(...)):

    if not file.filename.endswith(".csv"):
        return {"error": "Only CSV files are supported"}

    contents = await file.read()
    csv_string = contents.decode("utf-8")

    df = pd.read_csv(StringIO(csv_string))

    profile = profile_dataframe(df)

    analysis = generate_dataset_analysis(profile, df.columns.tolist())

    return {
        "filename": file.filename,
        "profile": profile,
        "analysis": analysis
    }