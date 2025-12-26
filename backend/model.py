import pandas as pd
import numpy as np
from datetime import datetime
dataset = pd.read_csv(r'../data/NepalLatestAQI.csv')
dataset.head()
dataset.isnull().sum()
dataset = dataset.drop(columns='notes')
dataset = dataset.rename(columns=lambda c: c.strip())
dataset = dataset.sort_values('date').drop_duplicates(subset=['station', 'date'], keep='last')
dataset['date'] = pd.to_datetime(dataset['date'])
dataset['month'] = dataset['date'].dt.month
dataset['day'] = dataset['date'].dt.day
dataset['dayofweek'] = dataset['date'].dt.dayofweek
dataset['is_weekend'] = dataset['dayofweek'].isin([5,6]).astype(int)
# ❗ Why do we encode cyclical features?

# Months and weekdays are cyclical:

# After December (12) comes January (1)

# After Sunday (6) comes Monday (0)

# If you use raw numbers:

# The model thinks January (1) is far from December (12)
# But they are next to each other.

dataset['month_sin'] = np.sin(2 * np.pi * dataset['month']/12)
dataset['month_cos'] = np.cos(2 * np.pi * dataset['month']/12)
dataset['dow_sin'] = np.sin(2 * np.pi * dataset['dayofweek']/7)
dataset['dow_cos'] = np.cos(2 * np.pi * dataset['dayofweek']/7)
dataset.columns
cols = ['pm2_5', 'pm10', 'no2', 'so2', 'o3', 'temperature_C', 'relative_humidity_%']
lags = [1, 3, 7]
rolls = [3, 7]
dataset = dataset.sort_values(['station', 'date']).reset_index(drop=True)
# Lags & rolling use only past values (shifted), so no leakage occurs

# create lag feature
for c in cols:
    for lag in lags:
        new_col = f"{c}_lag{lag}"
        dataset[new_col] = dataset.groupby('station')[c].shift(lag)
# crete rolling-mean features
for c in cols:
    for w in rolls:
        new_col = f"{c}_roll{w}"
        dataset[new_col] = (dataset.groupby('station')[c].transform(lambda s: s.rolling(window=w, min_periods=1).mean().shift(1)))
new_features = [c for c in dataset.columns if any(x in c for x in ['_lag', '_roll'])]
print(f"Added {len(new_features)} features: {new_features[:20]}{'' if len(new_features)<=20 else ' ...'}")
print("\nMissing values count for new features:")
print(dataset[new_features].isnull().sum())
initial_len = len(dataset)
dataset = dataset[~dataset[new_features].isnull().any(axis=1)].reset_index(drop=True)
dropped = initial_len - len(dataset)
print(f"Dropped {dropped} rows ({dropped/initial_len*100:.2f}%) because lag/roll features were not available.")
dataset['station'].nunique()
dataset = pd.get_dummies(dataset, columns=['station'], prefix='st', drop_first=False)
dataset.shape
train_end = '2025-05-30'
valid_end = '2025-08-30'
train_data = dataset[ dataset['date'] <= train_end ]
valid_data = dataset[ (dataset['date'] > train_end) & (dataset['date'] <= valid_end) ]
test_data = dataset[ dataset['date'] > valid_end ]
print("Train shape :", train_data.shape)
print("Valid shape :", valid_data.shape)
print("Test shape  :", test_data.shape)
target = 'aqi'
features = [c for c in dataset.columns if c not in ['date', 'aqi']]
x_train = train_data[features]
y_train = train_data[target]

x_valid = valid_data[features]
y_valid = valid_data[target]

x_test = test_data[features]
y_test = test_data[target]
from xgboost import XGBRegressor
model = XGBRegressor( n_estimators= 1844, 
learning_rate= 0.005138423447513474, 
max_depth= 6, 
subsample= 0.6011047426660859, 
colsample_bytree= 0.8897617488503717, 
reg_alpha= 0.0065837707426329595, 
reg_lambda= 8.5800788151842, 
min_child_weight= 2, 
gamma= 1.4098179177734331,
tree_method="hist", 
verbosity=0,
early_stopping_rounds=50)

model.fit(x_train, y_train, eval_set=[(x_valid, y_valid)], verbose=False)
from sklearn.metrics import mean_absolute_error, root_mean_squared_error
def compute_metrics(y_true, y_pred):
    mae = mean_absolute_error(y_true, y_pred)
    rmse = root_mean_squared_error(y_true, y_pred)
    mape = np.mean(np.abs((y_true - y_pred) / np.clip(y_true, 1e-6, None))) * 100
    return {'mae': mae, 'rmse': rmse, 'mape': mape}
pred_valid = model.predict(x_valid)
pred_test = model.predict(x_test)
metrics_valid = compute_metrics(y_valid, pred_valid)
metrics_test  = compute_metrics(y_test, pred_test)

print("Validation:", metrics_valid)
print("Test      :", metrics_test)
model.score(x_test, y_test)*100

import joblib
feature_names = x_train.columns.tolist()
joblib.dump(
    {"model": model, "features": feature_names},
    "model_bundle.pkl"
)