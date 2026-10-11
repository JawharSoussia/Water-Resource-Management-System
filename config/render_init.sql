CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS staging.stg_weather (
    weather_id TEXT PRIMARY KEY,
    city_name TEXT NOT NULL,
    country_code TEXT NOT NULL,
    temperature_celsius NUMERIC(5,2) NOT NULL,
    feels_like_celsius NUMERIC(5,2) NOT NULL,
    humidity_percent INTEGER NOT NULL,
    pressure_hpa NUMERIC(7,1) NOT NULL,
    wind_speed_ms NUMERIC(6,2) NOT NULL,
    visibility_meters INTEGER NOT NULL,
    weather_timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.stg_news (
    article_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    url TEXT NOT NULL,
    source_name TEXT NOT NULL,
    author TEXT,
    published_at TIMESTAMP NOT NULL,
    content TEXT,
    content_length INTEGER NOT NULL,
    domain TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
