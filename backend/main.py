from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
import joblib

app = FastAPI(title="Nepal AQI Prediction API")

# ---- CORS CONFIG (FIXES OPTIONS 405) ----
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # restrict later in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

model = joblib.load("model.pkl")

class AQIInput(BaseModel):
    pm2_5: float
    pm10: float
    no2: float
    so2: float
    o3: float
    temperature_C: float
    relative_humidity_: float
    month_sin: float
    month_cos: float
    dow_sin: float
    dow_cos: float

@app.get("/")
def health_check():
    return {"status": "API is running"}

@app.post("/predict")
def predict_aqi(data: AQIInput):
    df = pd.DataFrame([data.dict()])
    pred = model.predict(df)[0]
    return {"predicted_aqi": round(float(pred), 2)}
