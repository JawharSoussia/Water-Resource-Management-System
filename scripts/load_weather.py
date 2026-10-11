"""Fetch current Tunisia weather and refresh the dashboard warehouse tables."""

import os
from datetime import datetime

import pandas as pd
import requests
from sqlalchemy import create_engine, text


LOCATIONS = [
    ("Tunis", 36.8065, 10.1815),
    ("Sfax", 34.7406, 10.7600),
    ("Kairouan", 35.6781, 10.0963),
]


def database_engine():
    url = os.getenv("DATABASE_URL")
    if not url:
        user = os.getenv("POSTGRES_USER", "datauser")
        password = os.getenv("POSTGRES_PASSWORD", "datauser")
        host = os.getenv("POSTGRES_HOST", "localhost")
        database = os.getenv("POSTGRES_DB", "data_pipeline_db")
        url = f"postgresql://{user}:{password}@{host}/{database}"
    return create_engine(url.replace("postgres://", "postgresql://", 1), pool_pre_ping=True)


def fetch_weather():
    latitudes = ",".join(str(location[1]) for location in LOCATIONS)
    longitudes = ",".join(str(location[2]) for location in LOCATIONS)
    response = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": latitudes,
            "longitude": longitudes,
            "current": "temperature_2m,relative_humidity_2m,pressure_msl,wind_speed_10m",
            "timezone": "Africa/Tunis",
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict):
        payload = [payload]

    rows = []
    for (city, _, _), observation in zip(LOCATIONS, payload):
        current = observation["current"]
        rows.append({
            "weather_id": f"open-meteo-{city.lower()}-{current['time']}",
            "city_name": city,
            "country_code": "TN",
            "temperature_celsius": current["temperature_2m"],
            "feels_like_celsius": current["temperature_2m"],
            "humidity_percent": current["relative_humidity_2m"],
            "pressure_hpa": current["pressure_msl"],
            "wind_speed_ms": round(current["wind_speed_10m"] / 3.6, 2),
            "visibility_meters": 10000,
            "weather_timestamp": datetime.fromisoformat(current["time"]),
        })
    return pd.DataFrame(rows)


def refresh_warehouse(engine, weather):
    with engine.begin() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS marts"))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS staging.stg_weather (
                weather_id TEXT PRIMARY KEY, city_name TEXT NOT NULL,
                country_code TEXT NOT NULL, temperature_celsius NUMERIC(5,2) NOT NULL,
                feels_like_celsius NUMERIC(5,2) NOT NULL, humidity_percent INTEGER NOT NULL,
                pressure_hpa NUMERIC(7,1) NOT NULL, wind_speed_ms NUMERIC(6,2) NOT NULL,
                visibility_meters INTEGER NOT NULL, weather_timestamp TIMESTAMP NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """))
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS marts.news_daily_metrics (
                published_date DATE NOT NULL,
                source_name TEXT NOT NULL,
                articles_count INTEGER NOT NULL DEFAULT 0,
                unique_authors INTEGER NOT NULL DEFAULT 0,
                avg_content_length NUMERIC(10, 2) NOT NULL DEFAULT 0
            )
        """))

    weather.to_sql("stg_weather", engine, schema="staging", if_exists="replace", index=False)

    with engine.begin() as connection:
        connection.execute(text("DROP TABLE IF EXISTS marts.weather_enriched"))
        connection.execute(text("""
            CREATE TABLE marts.weather_enriched AS
            SELECT *, ROUND((temperature_celsius + 0.3)::numeric, 2) AS heat_index_celsius,
                CASE WHEN temperature_celsius < 10 THEN 'Cold'
                     WHEN temperature_celsius < 20 THEN 'Cool'
                     WHEN temperature_celsius < 30 THEN 'Warm' ELSE 'Hot' END AS temperature_category,
                (temperature_celsius > 35)::int AS has_extreme_heat,
                (temperature_celsius < -15)::int AS has_extreme_cold,
                (wind_speed_ms > 10)::int AS has_high_wind,
                (visibility_meters < 1000)::int AS has_low_visibility,
                CURRENT_TIMESTAMP AS created_at
            FROM staging.stg_weather
        """))
        connection.execute(text("DROP TABLE IF EXISTS marts.weather_daily_summary"))
        connection.execute(text("""
            CREATE TABLE marts.weather_daily_summary AS
            SELECT DATE(weather_timestamp) AS weather_date, city_name, country_code,
                COUNT(*) AS observations_count,
                ROUND(AVG(temperature_celsius)::numeric, 2) AS avg_temperature_celsius,
                ROUND(MIN(temperature_celsius)::numeric, 2) AS min_temperature_celsius,
                ROUND(MAX(temperature_celsius)::numeric, 2) AS max_temperature_celsius,
                ROUND(AVG(humidity_percent)::numeric, 1) AS avg_humidity_percent,
                ROUND(AVG(pressure_hpa)::numeric, 1) AS avg_pressure_hpa,
                ROUND(AVG(wind_speed_ms)::numeric, 2) AS avg_wind_speed_ms,
                ROUND(AVG(visibility_meters)::numeric, 0) AS avg_visibility_meters,
                CURRENT_TIMESTAMP AS created_at
            FROM staging.stg_weather
            GROUP BY DATE(weather_timestamp), city_name, country_code
        """))


if __name__ == "__main__":
    engine = database_engine()
    weather = fetch_weather()
    refresh_warehouse(engine, weather)
    print(f"Loaded {len(weather)} live weather observations into PostgreSQL.")
