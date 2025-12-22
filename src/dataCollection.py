import requests
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
import time

API_KEY = "84efa7ac1d8e306b982d2ec74ea4d4d0"
BASE_URL = "https://api.openweathermap.org/data/2.5/air_pollution"

# Nepal locations (expand later)
LOCATIONS = {
    "Kathmandu": (27.7172, 85.3240),
    "Bhaktapur": (27.6710, 85.4298),
    "Lalitpur": (27.6644, 85.3188),
}

START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 1, 31)

def unix(dt):
    return int(dt.timestamp())

rows = []

for city, (lat, lon) in LOCATIONS.items():
    current = START_DATE

    while current < END_DATE:
        next_day = current + timedelta(days=1)

        params = {
            "lat": lat,
            "lon": lon,
            "start": unix(current),
            "end": unix(next_day),
            "appid": API_KEY
        }

        r = requests.get(BASE_URL, params=params)
        r.raise_for_status()

        data = r.json()

        for item in data.get("list", []):
            components = item["components"]
            rows.append({
                "city": city,
                "latitude": lat,
                "longitude": lon,
                "datetime_utc": datetime.utcfromtimestamp(item["dt"]),
                "pm25": components["pm2_5"],
                "pm10": components["pm10"],
                "no2": components["no2"],
                "o3": components["o3"],
                "so2": components["so2"],
                "co": components["co"]
            })

        current = next_day
        time.sleep(1)  # avoid rate limits

df = pd.DataFrame(rows)
df.to_csv("Nepal_AirQuality_OpenWeather_Jan2024.csv", index=False)

print("Saved Nepal_AirQuality_OpenWeather_Jan2024.csv")
print(df.head())
