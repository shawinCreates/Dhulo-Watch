from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

app = FastAPI(title="Dhulo Watch – AQI Dashboard")

# Enable CORS for local development / frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

import joblib

bundle = joblib.load("model_bundle.pkl")
model = bundle["model"]
FEATURES = bundle["features"]


# -------------------------------
# Load and preprocess data
# -------------------------------
_df = pd.read_csv("../data/CleanData.csv")
_df['date'] = pd.to_datetime(_df['date'])
_df['year'] = _df['date'].dt.year
_df['month'] = _df['date'].dt.month
_df['month_name'] = _df['date'].dt.month_name()

def season_from_month(m):
    if m in [12, 1, 2]:
        return "Winter"
    elif m in [3, 4, 5]:
        return "Pre-Monsoon"
    elif m in [6, 7, 8, 9]:
        return "Monsoon"
    else:
        return "Post-Monsoon"

_df['season'] = _df['month'].apply(season_from_month)

# -------------------------------
# Endpoints
# -------------------------------

@app.get("/stations")
def get_stations():
    """Return sorted list of stations"""
    return sorted(_df['station'].unique().tolist())


@app.get("/kpis")
def get_kpis(station: str):
    """Return KPI metrics for selected station"""
    df = _df[_df['station'] == station].copy()
    avg_aqi = round(df['aqi'].mean(), 1)
    max_aqi = int(df['aqi'].max())
    avg_pm25 = round(df['pm2_5'].mean(), 1)
    unhealthy_pct = round((df['aqi'] > 100).mean() * 100, 1)
    worst_season = df.groupby('season')['aqi'].mean().idxmax()
    
    return {
        "avg_aqi": avg_aqi,
        "max_aqi": max_aqi,
        "avg_pm25": avg_pm25,
        "unhealthy_pct": unhealthy_pct,
        "worst_season": worst_season,
        "days": len(df)
    }


import numpy as np

@app.get("/monthly")
def monthly_trend(station: str, year: int):
    df = _df[(_df['station'] == station) & (_df['year'] == year)]
    
    # Group by month_name in calendar order
    months_order = [
        'January','February','March','April','May','June',
        'July','August','September','October','November','December'
    ]
    
    g = df.groupby('month_name')['aqi'].mean().reindex(months_order)
    
    return {
        "labels": g.index.tolist(),
        "values": g.round(1).replace({np.nan: None}).tolist()  # Use replace instead of fillna(None)
    }



@app.get("/seasonal")
def seasonal_summary(station: str):
    """Average PM2.5, PM10, AQI per season"""
    df = _df[_df['station'] == station].copy()
    g = df.groupby('season').agg({
        'pm2_5': 'mean',
        'pm10': 'mean',
        'aqi': 'mean'
    }).round(1)

    return {
        "seasons": g.index.tolist(),
        "pm25": g['pm2_5'].tolist(),
        "pm10": g['pm10'].tolist(),
        "aqi": g['aqi'].tolist()
    }


@app.get("/station_aqi")
def station_aqi():
    """Average AQI per station"""
    g = _df.groupby('station')['aqi'].mean().round(1)
    return {
        "stations": g.index.tolist(),
        "aqi": g.values.tolist()
    }


@app.get("/weather")
def weather_interaction(station: str):
    """Return temperature, humidity, AQI time series for station"""
    df = _df[_df['station'] == station].copy()
    return {
        "temperature": df['temperature_C'].round(1).tolist(),
        "humidity": df['relative_humidity_%'].round(1).tolist(),
        "aqi": df['aqi'].tolist()
    }


@app.get("/unhealthy_days")
def unhealthy_days(station: str, threshold: int = 100):
    """Monthly count of days with AQI above threshold"""
    df = _df[_df['station'] == station].copy()
    df['month_year'] = df['date'].dt.to_period('M')
    g = df[df['aqi'] > threshold].groupby('month_year').size()

    # Fill missing months with 0
    idx = pd.period_range(df['month_year'].min(), df['month_year'].max(), freq='M')
    g = g.reindex(idx, fill_value=0)

    return {
        "labels": [str(x) for x in g.index],
        "values": g.values.tolist()
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

    # calendar
    row["month"] = target_date.month
    row["day"] = target_date.day
    row["dayofweek"] = target_date.dayofweek
    row["is_weekend"] = int(target_date.dayofweek in [5, 6])

    row["month_sin"] = np.sin(2 * np.pi * row["month"] / 12)
    row["month_cos"] = np.cos(2 * np.pi * row["month"] / 12)
    row["dow_sin"] = np.sin(2 * np.pi * row["dayofweek"] / 7)
    row["dow_cos"] = np.cos(2 * np.pi * row["dayofweek"] / 7)

    # lags & rolling (same as training)
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

    for s in _df["station"].unique():
        row[f"st_{s}"] = 1 if s == station else 0

    X = pd.DataFrame([row])
    X = X.reindex(columns=FEATURES, fill_value=0)
    return X

@app.get("/forecast")
def forecast_aqi(station: str, days: int = 7):
    df = _df[_df["station"] == station].sort_values("date").copy()

    preds = []
    labels = []

    start_date = pd.Timestamp.today().normalize()

    for i in range(days):
        target_date = start_date + pd.Timedelta(days=i)

        X = build_latest_features(df, station, target_date)
        y_hat = float(model.predict(X)[0])

        preds.append(round(y_hat, 1))
        labels.append(str(target_date.date()))

        new_row = df.iloc[-1].copy()
        new_row["date"] = target_date
        new_row["aqi"] = y_hat

        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

    return {
        "labels": labels,
        "values": preds
    }
