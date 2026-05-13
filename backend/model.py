import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import joblib

dataset = pd.read_csv("../data/NepalLatestAQI.csv")
dataset = dataset.drop(columns="notes")
dataset = dataset.rename(columns=lambda c: c.strip())
dataset = dataset.sort_values("date").drop_duplicates(subset=["station", "date"], keep="last")

dataset["date"] = pd.to_datetime(dataset["date"])
dataset["month"] = dataset["date"].dt.month
dataset["day"] = dataset["date"].dt.day
dataset["dayofweek"] = dataset["date"].dt.dayofweek
dataset["is_weekend"] = (dataset["dayofweek"] >= 5).astype(int)

dataset["month_sin"] = np.sin(2 * np.pi * dataset["month"] / 12)
dataset["month_cos"] = np.cos(2 * np.pi * dataset["month"] / 12)
dataset["dow_sin"] = np.sin(2 * np.pi * dataset["dayofweek"] / 7)
dataset["dow_cos"] = np.cos(2 * np.pi * dataset["dayofweek"] / 7)

pollutants = ["pm2_5", "pm10", "no2", "so2", "o3", "temperature_C", "relative_humidity_%"]
lags = [1, 3, 7]
rolls = [3, 7]

dataset = dataset.sort_values(["station", "date"]).reset_index(drop=True)

for c in pollutants:
    for lag in lags:
        dataset[f"{c}_lag{lag}"] = dataset.groupby("station")[c].shift(lag)
    for w in rolls:
        dataset[f"{c}_roll{w}"] = (
            dataset.groupby("station")[c]
            .transform(lambda s: s.rolling(w, min_periods=1).mean().shift(1))
        )

for lag in lags:
    dataset[f"aqi_lag{lag}"] = dataset.groupby("station")["aqi"].shift(lag)

HORIZONS = [0, 1, 3, 7]
for h in HORIZONS:
    dataset[f"aqi_t_plus_{h}"] = dataset.groupby("station")["aqi"].shift(-h)

feature_cols = [c for c in dataset.columns if "_lag" in c or "_roll" in c]
target_cols = [f"aqi_t_plus_{h}" for h in HORIZONS]
dataset = dataset.dropna(subset=feature_cols + target_cols).reset_index(drop=True)

dataset = pd.get_dummies(dataset, columns=["station"], prefix="st", drop_first=False)

train_end = "2025-05-30"
valid_end = "2025-08-30"

train = dataset[dataset["date"] <= train_end]
valid = dataset[(dataset["date"] > train_end) & (dataset["date"] <= valid_end)]
test  = dataset[dataset["date"] > valid_end]

exclude_cols = ["date", "aqi"] + target_cols
features = [c for c in dataset.columns if c not in exclude_cols]

models = {}
for h in HORIZONS:
    print(f"Training AQI t+{h} model")
    target = f"aqi_t_plus_{h}"

    model = XGBRegressor(
        n_estimators=1800,
        learning_rate=0.005,
        max_depth=6,
        subsample=0.6,
        colsample_bytree=0.9,
        reg_alpha=0.01,
        reg_lambda=8,
        gamma=1.2,
        min_child_weight=2,
        tree_method="hist",
        early_stopping_rounds=50,
        verbosity=0
    )

    model.fit(
        train[features], train[target],
        eval_set=[(valid[features], valid[target])],
        verbose=False
    )

    pred_test = model.predict(test[features])
    metrics = {
        "mae": mean_absolute_error(test[target], pred_test),
        "rmse": np.sqrt(mean_squared_error(test[target], pred_test)),
        "mape": np.mean(np.abs((test[target] - pred_test) / np.clip(test[target], 1e-6, None))) * 100,
        "r2_pct": model.score(test[features], test[target]) * 100
    }

    print(f"t+{h} metrics:", metrics)
    models[h] = model

joblib.dump({
    "models": models,
    "features": features,
    "horizons": HORIZONS
}, "model_bundle_multistep.pkl")

print("\nSaved model_bundle_multistep.pkl")
