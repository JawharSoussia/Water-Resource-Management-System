-- Create schema for streaming staging tables
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- NOAA Water stream table
CREATE TABLE IF NOT EXISTS staging.water_stream (
    id SERIAL PRIMARY KEY,
    dataset_id VARCHAR(100) NOT NULL,
    temperature DECIMAL(8, 2),
    precipitation DECIMAL(8, 2),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    timestamp TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_water_stream_dataset ON staging.water_stream(dataset_id);
CREATE INDEX IF NOT EXISTS idx_water_stream_timestamp ON staging.water_stream(timestamp);

-- OpenWeather stream table
CREATE TABLE IF NOT EXISTS staging.weather_stream (
    id SERIAL PRIMARY KEY,
    location VARCHAR(100) NOT NULL,
    temperature DECIMAL(5, 2),
    humidity INTEGER,
    pressure INTEGER,
    wind_speed DECIMAL(5, 2),
    rain_mm DECIMAL(5, 2),
    timestamp TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT check_humidity CHECK (humidity >= 0 AND humidity <= 100)
);

CREATE INDEX IF NOT EXISTS idx_weather_stream_location ON staging.weather_stream(location);
CREATE INDEX IF NOT EXISTS idx_weather_stream_timestamp ON staging.weather_stream(timestamp);

-- Government alerts stream table
CREATE TABLE IF NOT EXISTS staging.alerts_stream (
    id SERIAL PRIMARY KEY,
    alert_type VARCHAR(50),
    severity VARCHAR(20),
    regions TEXT[],
    message TEXT,
    timestamp TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_alerts_stream_severity ON staging.alerts_stream(severity);
CREATE INDEX IF NOT EXISTS idx_alerts_stream_timestamp ON staging.alerts_stream(timestamp);

-- Stream consumer lag monitoring
CREATE TABLE IF NOT EXISTS monitoring.consumer_lag (
    id SERIAL PRIMARY KEY,
    consumer_group VARCHAR(100),
    topic VARCHAR(100),
    partition INTEGER,
    current_offset BIGINT,
    log_end_offset BIGINT,
    lag BIGINT,
    measured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_consumer_lag_topic ON monitoring.consumer_lag(topic, consumer_group);