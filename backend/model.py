import pandas as pd
import numpy as np
from datetime import datetime

dataset = pd.read_csv(r'../data/NepalLatestAQI.csv')

dataset = dataset.drop(columns='notes')
dataset = dataset.rename(columns=lambda c: c.strip())
dataset = dataset.sort_values('date').drop_duplicates(
    subset=['station', 'date'], keep='last'
)

dataset['date'] = pd.to_datetime(dataset['date'])

dataset['month'] = dataset['date'].dt.month
dataset['day'] = dataset['date'].dt.day
dataset['dayofweek'] = dataset['date'].dt.dayofweek
dataset['is_weekend'] = dataset['dayofweek'].isin([5, 6]).astype(int)

dataset['month_sin'] = np.sin(2 * np.pi * dataset['month'] / 12)
dataset['month_cos'] = np.cos(2 * np.pi * dataset['month'] / 12)
dataset['dow_sin'] = np.sin(2 * np.pi * dataset['dayofweek'] / 7)
dataset['dow_cos'] = np.cos(2 * np.pi * dataset['dayofweek'] / 7)

pollutant_cols = [
    'pm2_5', 'pm10', 'no2', 'so2', 'o3',
    'temperature_C', 'relative_humidity_%'
]

lags = [1, 3, 7]
rolls = [3, 7]

dataset = dataset.sort_values(['station', 'date']).reset_index(drop=True)

for c in pollutant_cols:
    for lag in lags:
        dataset[f"{c}_lag{lag}"] = (
            dataset.groupby('station')[c].shift(lag)
        )

for c in pollutant_cols:
    for w in rolls:
        dataset[f"{c}_roll{w}"] = (
            dataset.groupby('station')[c]
            .transform(lambda s: s.rolling(w, min_periods=1).mean().shift(1))
        )

aqi_lags = [1, 3, 7]

for lag in aqi_lags:
    dataset[f"aqi_lag{lag}"] = (
        dataset.groupby('station')['aqi'].shift(lag)
    )

lag_roll_cols = [c for c in dataset.columns if '_lag' in c or '_roll' in c]

initial_len = len(dataset)
dataset = dataset[~dataset[lag_roll_cols].isnull().any(axis=1)].reset_index(drop=True)
dropped = initial_len - len(dataset)

print(f"Dropped {dropped} rows ({dropped / initial_len * 100:.2f}%) due to lag/roll features")

dataset = pd.get_dummies(dataset, columns=['station'], prefix='st', drop_first=False)

train_end = '2025-05-30'
valid_end = '2025-08-30'

train_data = dataset[dataset['date'] <= train_end]
valid_data = dataset[(dataset['date'] > train_end) & (dataset['date'] <= valid_end)]
test_data  = dataset[dataset['date'] > valid_end]

print("Train shape:", train_data.shape)
print("Valid shape:", valid_data.shape)
print("Test  shape:", test_data.shape)

target = 'aqi'
features = [c for c in dataset.columns if c not in ['date', 'aqi']]

x_train, y_train = train_data[features], train_data[target]
x_valid, y_valid = valid_data[features], valid_data[target]
x_test,  y_test  = test_data[features],  test_data[target]

from xgboost import XGBRegressor

model = XGBRegressor(
    n_estimators=1844,
    learning_rate=0.005138423447513474,
    max_depth=6,
    subsample=0.6011047426660859,
    colsample_bytree=0.8897617488503717,
    reg_alpha=0.0065837707426329595,
    reg_lambda=8.5800788151842,
    min_child_weight=2,
    gamma=1.4098179177734331,
    tree_method="hist",
    verbosity=0,
    early_stopping_rounds=50
)

model.fit(
    x_train, y_train,
    eval_set=[(x_valid, y_valid)],
    verbose=False
)

from sklearn.metrics import mean_absolute_error, root_mean_squared_error

def compute_metrics(y_true, y_pred):
    return {
        "mae": mean_absolute_error(y_true, y_pred),
        "rmse": root_mean_squared_error(y_true, y_pred),
        "mape": np.mean(
            np.abs((y_true - y_pred) / np.clip(y_true, 1e-6, None))
        ) * 100
    }

pred_valid = model.predict(x_valid)
pred_test  = model.predict(x_test)

print("Validation:", compute_metrics(y_valid, pred_valid))
print("Test      :", compute_metrics(y_test, pred_test))
print("R² (%)    :", model.score(x_test, y_test) * 100)

import joblib

joblib.dump(
    {
        "model": model,
        "features": features
    },
    "model_bundle.pkl"
)
