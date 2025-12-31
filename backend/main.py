from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
import joblib

app = FastAPI(title="Dhulo Watch – AQI Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

bundle = joblib.load("model_bundle.pkl")
model = bundle["model"]
FEATURES = bundle["features"]

_df = pd.read_csv("../data/CleanData.csv")
_df["date"] = pd.to_datetime(_df["date"])
_df["year"] = _df["date"].dt.year
_df["month"] = _df["date"].dt.month
_df["month_name"] = _df["date"].dt.month_name()

def season_from_month(m):
    if m in [12, 1, 2]:
        return "Winter"
    elif m in [3, 4, 5]:
        return "Pre-Monsoon"
    elif m in [6, 7, 8, 9]:
        return "Monsoon"
    else:
        return "Post-Monsoon"

_df["season"] = _df["month"].apply(season_from_month)

@app.get("/stations")
def get_stations():
    return sorted(_df["station"].unique().tolist())


@app.get("/kpis")
def get_kpis(station: str):
    df = _df[_df["station"] == station]
    return {
        "avg_aqi": round(df["aqi"].mean(), 1),
        "max_aqi": int(df["aqi"].max()),
        "avg_pm25": round(df["pm2_5"].mean(), 1),
        "unhealthy_pct": round((df["aqi"] > 100).mean() * 100, 1),
        "worst_season": df.groupby("season")["aqi"].mean().idxmax(),
        "days": len(df),
    }


@app.get("/monthly")
def monthly_trend(station: str, year: int):
    df = _df[(_df["station"] == station) & (_df["year"] == year)]

    months_order = [
        "January","February","March","April","May","June",
        "July","August","September","October","November","December"
    ]

    g = df.groupby("month_name")["aqi"].mean().reindex(months_order)

    return {
        "labels": g.index.tolist(),
        "values": g.round(1).replace({np.nan: None}).tolist()
    }


@app.get("/seasonal")
def seasonal_summary(station: str):
    g = (
        _df[_df["station"] == station]
        .groupby("season")
        .agg({"pm2_5": "mean", "pm10": "mean", "aqi": "mean"})
        .round(1)
    )

    return {
        "seasons": g.index.tolist(),
        "pm25": g["pm2_5"].tolist(),
        "pm10": g["pm10"].tolist(),
        "aqi": g["aqi"].tolist(),
    }


@app.get("/station_aqi")
def station_aqi():
    g = _df.groupby("station")["aqi"].mean().round(1)
    return {"stations": g.index.tolist(), "aqi": g.tolist()}


@app.get("/weather")
def weather_interaction(station: str):
    df = _df[_df["station"] == station]
    return {
        "temperature": df["temperature_C"].round(1).tolist(),
        "humidity": df["relative_humidity_%"].round(1).tolist(),
        "aqi": df["aqi"].tolist(),
    }


@app.get("/unhealthy_days")
def unhealthy_days(station: str, threshold: int = 100):
    df = _df[_df["station"] == station].copy()
    df["month_year"] = df["date"].dt.to_period("M")

    g = df[df["aqi"] > threshold].groupby("month_year").size()
    idx = pd.period_range(df["month_year"].min(), df["month_year"].max(), freq="M")
    g = g.reindex(idx, fill_value=0)

    return {
        "labels": [str(x) for x in g.index],
        "values": g.values.tolist(),
    }

def build_latest_features(df: pd.DataFrame, station: str, target_date: pd.Timestamp):
    df = df.sort_values("date")
    latest = df.iloc[-1]

    row = {}

    base_cols = [
        "latitude", "longitude",
        "pm2_5", "pm10", "no2", "so2", "o3",
        "temperature_C", "relative_humidity_%"
    ]

    for c in base_cols:
        row[c] = latest[c]

    row["month"] = target_date.month
    row["day"] = target_date.day
    row["dayofweek"] = target_date.dayofweek
    row["is_weekend"] = int(target_date.dayofweek in [5, 6])

    row["month_sin"] = np.sin(2 * np.pi * row["month"] / 12)
    row["month_cos"] = np.cos(2 * np.pi * row["month"] / 12)
    row["dow_sin"] = np.sin(2 * np.pi * row["dayofweek"] / 7)
    row["dow_cos"] = np.cos(2 * np.pi * row["dayofweek"] / 7)

    lag_cols = [
        "pm2_5","pm10","no2","so2","o3",
        "temperature_C","relative_humidity_%"
    ]

    for c in lag_cols:
        row[f"{c}_lag1"] = df[c].iloc[-1]
        row[f"{c}_lag3"] = df[c].iloc[-3]
        row[f"{c}_lag7"] = df[c].iloc[-7]
        row[f"{c}_roll3"] = df[c].iloc[-3:].mean()
        row[f"{c}_roll7"] = df[c].iloc[-7:].mean()

    row["aqi_lag1"] = df["aqi"].iloc[-1]
    row["aqi_lag3"] = df["aqi"].iloc[-3]
    row["aqi_lag7"] = df["aqi"].iloc[-7]

    for s in _df["station"].unique():
        row[f"st_{s}"] = int(s == station)

    X = pd.DataFrame([row])
    return X.reindex(columns=FEATURES, fill_value=0)

@app.get("/forecast")
def forecast_aqi(station: str, days: int = 7):
    df = _df[_df["station"] == station].sort_values("date").copy()

    preds = []
    labels = []

    start_date = df["date"].max() + pd.Timedelta(days=1)

    DRIFT = {
        "pm2_5": 0.05,
        "pm10": 0.04,
        "no2": 0.03,
        "so2": 0.02,
        "o3": 0.03,
        "temperature_C": 0.01,
        "relative_humidity_%": 0.01,
    }

    for i in range(days):
        target_date = start_date + pd.Timedelta(days=i)

        X = build_latest_features(df, station, target_date)
        y_hat = float(model.predict(X)[0])

        preds.append(round(y_hat, 1))
        labels.append(str(target_date.date()))

        last = df.iloc[-1].copy()

        for col, pct in DRIFT.items():
            trend = (
                df[col].iloc[-7:].mean()
                - df[col].iloc[-14:-7].mean()
                if len(df) >= 14
                else 0
            )
            last[col] = max(0, last[col] + trend * pct)

        last["aqi"] = y_hat
        last["date"] = target_date

        df = pd.concat([df, pd.DataFrame([last])], ignore_index=True)

    return {
        "labels": labels,
        "values": preds
    }
