-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- Create audit tables
CREATE TABLE IF NOT EXISTS public.dbt_tests (
    id SERIAL PRIMARY KEY,
    test_name VARCHAR(255),
    status VARCHAR(50),
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create weather streaming table
CREATE TABLE IF NOT EXISTS staging.weather_stream (
    id SERIAL PRIMARY KEY,
    location VARCHAR(255) NOT NULL,
    temperature FLOAT,
    humidity INT,
    pressure INT,
    wind_speed FLOAT,
    rain_mm FLOAT,
    timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create water streaming table
CREATE TABLE IF NOT EXISTS staging.water_stream (
    id SERIAL PRIMARY KEY,
    dataset_id VARCHAR(255),
    temperature FLOAT,
    precipitation FLOAT,
    timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create monitoring table for consumer lag
CREATE TABLE IF NOT EXISTS monitoring.consumer_lag (
    id SERIAL PRIMARY KEY,
    consumer_group VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    partition INT NOT NULL,
    current_offset BIGINT,
    log_end_offset BIGINT,
    lag BIGINT,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA raw TO datauser;
GRANT ALL PRIVILEGES ON SCHEMA staging TO datauser;
GRANT ALL PRIVILEGES ON SCHEMA marts TO datauser;
GRANT ALL PRIVILEGES ON SCHEMA monitoring TO datauser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO datauser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA monitoring TO datauser;