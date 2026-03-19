# Data Ingestion Documentation

## Overview

This document describes how data is ingested from external APIs into the MinIO data lake.

## Configured Data Sources

### 1. OpenWeather API
- **URL:** https://api.openweathermap.org/data/2.5/weather
- **Frequency:** Every 6 hours (via Airflow)
- **Cities:** Configurable via OPENWEATHER_CITIES in .env
- **Fields:** Temperature, humidity, pressure, visibility, wind speed
- **Authentication:** API key in OPENWEATHER_API_KEY

### 2. NOAA Global Water API (No API key needed!)
- **URL:** https://www.ncei.noaa.gov/api
- **Region:** MENA (Middle East & North Africa)
- **Frequency:** Every 6 hours (via Airflow)
- **Datasets:** Gridded climate & water data (nclimgrid)
- **Coverage:** Tunisia, North Africa coordinates-based data
- **Fields:** Temperature, precipitation, water-related parameters
- **Authentication:** None! Public NOAA data

## Ingestion Flow

```
[API Endpoint]
      ↓
[Python Script]
      ├─ Fetch data with retry logic (max 3 attempts)
      ├─ Validate required fields
      ├─ Add ingestion metadata
      └─ Upload to MinIO
      ↓
[MinIO: aqua-raw-data bucket]
```

## Error Handling

All ingestion scripts implement:
- **Retry Logic:** Up to 3 attempts with 5-second delays
- **Logging:** Structured logs with timestamps and levels
- **Validation:** Required fields checked before upload
- **Timeout:** 10-second request timeout per API call

## MonitoringChecks

Before moving to the transformation phase, verify:
1. All data successfully uploaded to MinIO
2. No validation errors in logs
3. All tests passing (pytest tests/)
4. API keys configured in .env
5. Docker services running (docker-compose ps)

## Future Enhancements

- Add data schema validation (pydantic)
- Implement data quality metrics
- Add alerting for failed ingestions
- Parallel API requests for multiple cities
```